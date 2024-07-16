# Parsing technical CSV files on FTP servers for each hardcode technology,tasks in the script
# Get 2 arguments: download_datetime and table_type
# If table_type is Temp, the script parses the entire date and uploads it to the tech.tech_?_history table
# If table_type is History defined then the script parses files after time and uploads it to the tech.tech_?_temp table
# !!! Needed to change parse_object_name function for specific technology !!!

import argparse
import paramiko
import pandas as pd
import io
import os
from sqlalchemy.engine import create_engine
from sqlalchemy.types import Integer
from time import perf_counter
from collections import defaultdict


COMMON_COLUMNS = ['Result Time', 'Object Name']
TASKS_COLUMNS = {
    '4g': {
        '1526726660': ['1526727547', '1526727546', '1526726686', '1526726687'],
        '1526726657': ['1526728217','1526728218','1526728221','1526728219','1526728222','1526728223','1526728226','1526728224'],
        '1526726659': ['1526726669', '1526726668'],
    }
}


def to_greenplum(data, engine, table_type, download_datetime, if_exists, schema='tech', index=False):
    for technology in data:
        df = data[technology][sorted(data[technology].columns, reverse=True)]
        df = df.dropna(subset=['Object Name'])
        df.rename(columns={'Object Name': 'object_name', 'Result Time': 'result_time'}, inplace=True)
        if table_type == 'history':
            df.insert(0, 'pdate', download_datetime[:-4])

        df.to_sql(f'tech_{technology}_{table_type}', engine, schema=schema, if_exists=if_exists, index=index, index_label=None,
                  dtype={k: Integer() for k in df.columns if k.isnumeric()})
        print(f'Success upload {df.shape[0]} rows to {schema}.tech_{technology}_{table_type} table')

def merge_dfs(data, common_columns):
    result_df = None
    for task in data:
        if result_df is not None:
            result_df = result_df.merge(data[task].assign(task=task),  how='outer', on=common_columns)
        else:
            result_df = data[task].assign(task=task)

    return result_df


def parse_object_name(data, task, time):
    d = defaultdict(lambda: None)
    try:
        ne, params = data.split("/")
        for param in params.split(", "):
            k, v = param.split('=')
            k = 'Cell:Label' if 'Cell:Label' in k else k
            d[k] = v
        cell_id, cell_name = d['Local Cell ID'], d['Cell Name']
        cell_node_name, node_id, cell_fdd_tdd = d['Cell:eNodeB Function Name'], d['eNodeB ID'], d['Cell FDD TDD indication']
    except Exception as e:
        print(f'there is wrong data in string {data} in task {task} at {time}')
        print(e.args)
        return None, None, None, None, None, None

    return ne, cell_id, cell_name, cell_node_name, node_id, cell_fdd_tdd


def get_ssh_client():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    return ssh


def download_from_sftp(hosts, port, username, password, download_datetime, common_columns, tasks_columns):
    print('Download date:', download_datetime)
    files_to_lists = {technology: {} for technology in tasks_columns}

    for host in hosts:
        try:
            print(f'Downloading from {host}')
            ssh = get_ssh_client()
            ssh.connect(host, port, username, password)
            sftp = ssh.open_sftp()
            directory = f'/export/home/omc/var/fileint/pm/pmexport_{download_datetime[:-4]}/'
            for technology in tasks_columns:
                for pattern, columns in tasks_columns[technology].items():
                    files = [f for f in sftp.listdir(directory) if pattern in f]
                    data = [
                            pd.read_csv(
                                io.BytesIO(sftp.open(os.path.join(directory, file)).read()),
                                sep=',',
                                on_bad_lines='skip',
                                index_col=False,
                                dtype='unicode',
                                skiprows=[1],
                                usecols=common_columns + columns
                            )
                            for file in files if file[-16: -3] >= download_datetime
                            ]
                    files_to_lists[technology][pattern] = files_to_lists[technology].get(pattern, []) + data
            sftp.close()
            ssh.close()
        except FileNotFoundError as e:
            print(f'Wrong directory or the file directory = {directory} on the host {host}')
            raise e
        print(f'Success download from the host {host}')
    return files_to_lists



if __name__ == '__main__':
    start_time = perf_counter()
    parser_parameters = argparse.ArgumentParser(description="Process some parameters.")
    parser_parameters.add_argument('parameters', metavar='download_datetime, table_type', type=str, nargs='+')
    download_datetime, table_type = parser_parameters.parse_args().parameters
    # download_datetime, table_type = '202406020000', 'history'
    if table_type not in ('history', 'temp'):
        raise KeyError(f'table_type must be history or temp not: {table_type}')

    username, password = ***, ***
    hosts = [
            '172.30.45.233',
            '172.30.45.234',
            '172.30.45.235',
            '172.30.45.236',
            '172.30.45.238'
             ]
    port = 22
    gp_engine = create_engine('postgresql://***')

    list_of_dfs = download_from_sftp(hosts, port, username, password, download_datetime, COMMON_COLUMNS, TASKS_COLUMNS)
    print('Start merging:')
    for technology in list_of_dfs:
        for task in list_of_dfs[technology]:
            list_of_dfs[technology][task] = pd.concat(list_of_dfs[technology][task])
        list_of_dfs[technology] = merge_dfs(list_of_dfs[technology], ['Result Time', 'Object Name', 'task'])
        print('Success merge:', technology)
        print('Start parsing:')
        list_of_dfs[technology][['ne', 'cell_id', 'cell_name', 'cell_node_name', 'node_id', 'cell_fdd_tdd']] = (
            list_of_dfs[technology]
            .apply(lambda x: pd.Series(parse_object_name(x['Object Name'], x['task'], x['Result Time'])), axis=1))
        list_of_dfs[technology] = list_of_dfs[technology].drop(['task'], axis=1)
        list_of_dfs[technology] = list_of_dfs[technology].assign(tech=technology)
        print('Success parsing:', technology)


    print('Start uploading to Greenplum: ')
    to_greenplum(list_of_dfs, gp_engine, table_type, download_datetime,
                 if_exists='append' if table_type == 'history' else 'replace',
                 schema='tech')
    print(f'Duration for date {download_datetime}:', round((perf_counter() - start_time) / 60, 2), 'min')

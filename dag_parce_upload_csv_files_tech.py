from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.decorators import task
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email_operator import EmailOperator
#from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta, datetime
import pandas as pd


@task
def create_kpi_file(**context):
	pass


@task.branch(trigger_rule='none_failed')
def check_kpi_needed(**context):
	pass


@task.branch
def get_mask(**context):
	pass


@task.branch
def check_kpi_validate(**context):
	pass


default_args = {
    'start_date': datetime(2024, 5, 30),
    'retries': 0,
    'retry_delay': timedelta(minutes=30),
    'email_on_failure': True,
    'email': ['DSuchkov@beeline.uz'],
    'do_xcom_push': False,
    'owner': 'Denis',
}

with DAG(
        dag_id='tech_parser_ftp_gp_kpi_4_hour',
        default_args=default_args,
        schedule_interval='25 0/4 * * *',
        catchup=False,
        # max_active_runs=1,
        tags=['tech'],
        params={'upload_kpi': True,
                'do_parse': True,
                'start_kpi_period': None,
                'end_kpi_period': None,
                'download_datetime_YmdH00': None,
                'table_type': 'temp',
                },
) as dag:

    get_mask = get_mask()

    create_new_partition = PostgresOperator(

    )


    parser_2g = SSHOperator(

    )

    parser_3g = SSHOperator(

    )

    parser_4g = SSHOperator(

    )

    ftp_upload_file = SSHOperator(

    )

    send_email = EmailOperator(

    )

    create_kpi_file = create_kpi_file(retries=2, retry_delay=timedelta(minutes=12), email=['dsuchkov@beeline.uz', 'ZhASaidov@beeline.uz', 'DSayfulin@beeline.uz', 'SRakhmatullaev@beeline.uz'])
    check_kpi_needed = check_kpi_needed()
    check_kpi_validate = check_kpi_validate()

    get_mask >> [parser_2g, parser_3g, parser_4g] >> check_kpi_needed >> create_kpi_file >> check_kpi_validate
    get_mask >> create_new_partition >> [parser_2g, parser_3g, parser_4g]
    check_kpi_validate >> send_email >> ftp_upload_file
    check_kpi_validate >> ftp_upload_file
    get_mask >> check_kpi_needed

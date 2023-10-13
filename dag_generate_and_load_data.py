from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.decorators import task
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator

#from airflow.operators.python import BranchPythonOperator

from datetime import timedelta, datetime


default_args = {
    'start_date':  datetime(2023, 10, 12),
    'retries': 4,
    'retry_delay': timedelta(minutes=30)
}


@task
def get_mask(**context):
    pg_hook = PostgresHook(postgres_conn_id='***')
    sql_query = "select max(time_key) from tmp.agg_***;"
    # SET LOAD DATA DATE
    # next_file_date = '2023-10-02'
    next_file_date = pg_hook.get_first(sql_query)[0].strftime('%Y-%m-%d')

    print(next_file_date, '<----------------- mask')

    context['ti'].xcom_push(key='mask', value=next_file_date)


with DAG(
        dag_id='dag_evolution_generate_profile_new',
        default_args=default_args,
        schedule_interval='@continuous',
        catchup=False,
        max_active_runs=1,
        tags=['***'],
    ) as dag:

    check_profile_flag = FileSensor(
        task_id="check_profile_flag",
        filepath='***.OK',
        fs_conn_id='***',
        poke_interval=60 * 5,
    )

    create_evol_profile_new = PostgresOperator(
        task_id='create_evol_profile_new',
        postgres_conn_id='***',
        sql="sql/***.sql",
        hook_params={'options': '-c statement_timeout=5000s'},
    )

    create_csv_profile = PostgresOperator(
        task_id='create_csv_profile',
        postgres_conn_id='***',
        sql="sql/***.sql",
        hook_params={'options': '-c statement_timeout=5000s'}
    )

    create_csv_canceled = PostgresOperator(
        task_id='create_csv_canceled',
        postgres_conn_id='***',
        sql="""COPY (
        ***
        ***
        ***
            )
            TO '*** canceled_subs_{{ dag.timezone.convert(execution_date).strftime("%Y%m%d") }}.csv' with csv delimiter '|'""",
        hook_params={'options': '-c statement_timeout=5000s'}
    )

    upload_canceled_to_evol = SSHOperator(
        task_id='upload_canceled_to_evol.sh',
        ssh_conn_id='***',
        command='bash *** {{ dag.timezone.convert(execution_date).strftime("%Y%m%d") }} ',
        do_xcom_push=False,
        cmd_timeout=3600
    )

    upload_profile_to_evol = SSHOperator(
        task_id='upload_profile_to_evol',
        ssh_conn_id='***',
        command='bash *** upload_profile.sh {{ dag.timezone.convert(execution_date).strftime("%Y%m%d") }} ',
        do_xcom_push=False,
        cmd_timeout=3600,
    )

    remove_flag = BashOperator(
        task_id='remove_flag',
        bash_command='rm ***',
        retries=6,
        trigger_rule='all_done',
        email_on_retry=True,
        email='***'

    )

    send_profile_success_upload = EmailOperator(
        task_id='send_profile_success_upload',
        to='***',
        subject='***',
        html_content='CDR  PART_FILE  for date {{ dag.timezone.convert(execution_date).strftime("%Y%m%d") }} was successfully uploaded.'
    )

    get_mask = get_mask()


    check_profile_flag >> get_mask >> create_evol_profile_new >> create_csv_profile >> upload_profile_to_evol >> remove_flag
    check_profile_flag >> create_csv_canceled >> upload_canceled_to_evol >> remove_flag
    upload_profile_to_evol >> send_profile_success_upload

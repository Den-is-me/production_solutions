from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.decorators import task
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.providers.oracle.operators.oracle import OracleOperator

from datetime import timedelta, datetime


default_args = {
    'start_date':  datetime(2023, 10, 12),
    'retries': 0,
    'retry_delay': timedelta(minutes=30),
    'email_on_failure': True,
    'email': ['***'],
    'do_xcom_push': False,
}


@task.branch
def get_mask(**context):
    pg_hook = PostgresHook(postgres_conn_id='***')
    sql_query = "select max(time_key) from ***;"
    # SET LOAD DATA DATE
    # dt = '2023-10-02'
    dt = pg_hook.get_first(sql_query)[0]
    next_file_date = dt.strftime('%Y-%m-%d')

    print(next_file_date, '<----------------- mask')

    context['ti'].xcom_push(key='mask', value=next_file_date)

    if context['dag'].timezone.convert(context['ti'].start_date).date() - dt > timedelta(days=7):
        return ['churn_alert', 'create_evol_profile_new']
    return 'create_evol_profile_new'

@task.branch
def check_morning_profile(**context):
    '''CHECK IF THE FILES ARE ALREADY UPLOADED TODAY
        IF NOT THEN LOAD ALL FILES
        ELSE LOAD ONLY PROFILE'''

    prev_start = context['ti'].previous_start_date_success
    if not prev_start or prev_start.date() != context['ti'].start_date.date():
        return ['get_mask', 'create_csv_canceled', 'run_proc_product_catalogue']
    return 'get_mask'


with DAG(
        dag_id='dag_evolution_generate_profile_new_test',
        default_args=default_args,
        schedule_interval='@continuous',
        catchup=False,
        max_active_runs=1,
        tags=['evolution'],
    ) as dag:

    check_profile_flag = FileSensor(
        task_id="check_profile_flag",
        filepath='den_dag/files/CVM_PART_PROFILE_DONE.OK',
        fs_conn_id='***',
        poke_interval=60 * 30,
    )

    check_morning_profile = check_morning_profile()

    get_mask = get_mask()

    create_evol_profile_new = PostgresOperator(
        task_id='create_evol_profile_new',
        postgres_conn_id='***',
        sql="sql/evolution/create_evol_profile_new.sql",
        hook_params={'options': '-c statement_timeout=5000s'},
        retries=1
    )

    create_csv_profile = PostgresOperator(
        task_id='create_csv_profile',
        postgres_conn_id='***',
        sql="sql/evolution/create_part_profile_csv.sql",
        hook_params={'options': '-c statement_timeout=5000s'},
    )

    create_csv_canceled = PostgresOperator(
        task_id='create_csv_canceled',
        postgres_conn_id='***',
        sql="""COPY (
            ***
            )
            TO '/data/evolution/canceled_subs_{{ dag.timezone.convert(ti.start_date).strftime("%Y%m%d") }}.csv' with csv delimiter ','""",
        hook_params={'options': '-c statement_timeout=5000s'}
    )

    upload_canceled_to_evol = SSHOperator(
        task_id='upload_canceled_to_evol.sh',
        ssh_conn_id='***',
        command='bash /data/evolution/evol_upload_canceled.sh {{ dag.timezone.convert(ti.start_date).strftime("%Y%m%d") }} ',
        do_xcom_push=False,
        cmd_timeout=3600,
        retries=1
    )

    upload_profile_to_evol = SSHOperator(
        task_id='upload_profile_to_evol',
        ssh_conn_id='***',
        command='bash /data/evolution/evol_upload_profile.sh {{ dag.timezone.convert(ti.start_date).strftime("%Y%m%d") }} ',
        do_xcom_push=False,
        cmd_timeout=3600,
        retries=1
    )

    remove_profile_flag = BashOperator(
        task_id='remove_profile_flag',
        bash_command='rm /opt/airflow/dags/den_dag/files/CVM_PART_PROFILE_DONE.OK',
        trigger_rule='one_success',
        retries=2,
        email_on_retry=True,
        email='***',

    )

    send_profile_success_upload = EmailOperator(
        task_id='send_profile_success_upload',
        to='***',
        subject='CDR  PART_FILE',
        html_content='CDR  PART_FILE  for date {{ dag.timezone.convert(ti.start_date).strftime("%Y%m%d") }} was successfully uploaded.',
    )

    run_proc_product_catalogue = OracleOperator(
        task_id='run_proc_product_catalogue',
        oracle_conn_id='***',
        sql="""begin
               biis_cis.cms_product_catalogue_prc;
            end;""",
    )

    create_csv_catalog = PostgresOperator(
        task_id='create_txt_catalog',
        postgres_conn_id='***',
        sql="""COPY (select * from tmp.cms_product_catalogue)
            TO '/data/evolution/product_catalogue_{{ dag.timezone.convert(ti.start_date).strftime("%Y%m%d") }}.txt' with csv delimiter '|'""",
        hook_params={'options': '-c statement_timeout=5000s'},
        retries=1,
    )

    upload_catalog_to_evol = SSHOperator(
        task_id='upload_catalog_to_evol',
        ssh_conn_id='***',
        command='bash /data/evolution/evol_upload_catalog.sh {{ dag.timezone.convert(ti.start_date).strftime("%Y%m%d") }} ',
        do_xcom_push=False,
        cmd_timeout=3600,
        retries=1
    )

    churn_alert = EmailOperator(
        task_id='churn_alert',
        to='***',
        subject='CHURN ALERT',
        html_content='Last Churn = {{ ti.xcom_pull(task_ids="get_mask", key="mask") }}.'
    )


    check_profile_flag >> check_morning_profile
    check_morning_profile >> get_mask >> create_evol_profile_new >> create_csv_profile >> upload_profile_to_evol >> remove_profile_flag
    check_morning_profile >> create_csv_canceled >> upload_canceled_to_evol >> remove_profile_flag
    check_morning_profile >> run_proc_product_catalogue >> create_csv_catalog >> upload_catalog_to_evol >> remove_profile_flag
    upload_profile_to_evol >> send_profile_success_upload
    get_mask >> churn_alert

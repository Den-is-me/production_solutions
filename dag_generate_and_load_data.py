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

}


@task.branch
def get_mask(**context):

    churn_dt = pg_hook_test.get_first(sql_query_churn_dt)[0]
    mask = churn_dt.strftime('%Y%m%d')

    profile_dt = pg_hook_prod.get_first(sql_query_profile_dt)[0]

    print(mask, '<----------------- mask')

    context['ti'].xcom_push(key='mask', value=mask)
    context['ti'].xcom_push(key='partition', value=profile_dt.strftime('%Y%m%d'))
    context['ti'].xcom_push(key='partition_exclusive', value=(profile_dt + timedelta(days=1)).strftime('%Y%m%d'))
    context['ti'].xcom_push(key='drop_partition', value=(profile_dt - timedelta(days=30)).strftime('%Y%m%d'))

    if context['dag'].timezone.convert(context['ti'].start_date).date() - churn_dt > timedelta(days=7):
        return ['churn_alert', 'create_evol_churn', 'create_new_partition']
    return ['create_evol_churn', 'create_new_partition']

@task
def check_device_field(**context):
    pg_hook = PostgresHook(postgres_conn_id='prod_greenplum_gpadmin')
    sql_query = "select count(1) from cvm.evol_profile where device_type is not Null;"
    check_result = pg_hook.get_first(sql_query)[0]

    print(check_result, '<----------------- amount of devices')

    if not check_result:
        raise ValueError('There is no value in the profile device field')


with DAG(
        dag_id='evol_generate_profile_morning',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False,
        max_active_runs=1,
        tags=['evolution'],
    ) as dag:

    check_profile_flag = FileSensor(

    )

    get_mask = get_mask()

    create_new_partition = PostgresOperator(

    )

    save_previous_profile_in_history = PostgresOperator(

    )

    drop_old_partitions = PostgresOperator(

    )

    create_profile_flag_table = PostgresOperator(
    )
    churn_alert = EmailOperator(

    )

    create_evol_churn = PostgresOperator(

    )

    create_evol_profile = PostgresOperator(

    )

    create_csv_profile = PostgresOperator(

    )

    create_csv_canceled = PostgresOperator(

    )

    upload_canceled_to_evol = SSHOperator(

    )

    upload_profile_to_evol = SSHOperator(

    )

    remove_profile_flag = BashOperator(


    )

    send_profile_success_upload = EmailOperator(

    )

    run_proc_product_catalogue = OracleOperator(

    )

    create_csv_catalog = PostgresOperator(

    )

    upload_catalog_to_evol = SSHOperator(

    )

    check_device_field = check_device_field()




    check_profile_flag >> get_mask >> create_evol_churn >> create_evol_profile >> create_csv_profile >> upload_profile_to_evol >> remove_profile_flag
    check_profile_flag >> create_csv_canceled >> upload_canceled_to_evol >> remove_profile_flag
    check_profile_flag >> run_proc_product_catalogue >> create_csv_catalog >> upload_catalog_to_evol >> remove_profile_flag
    check_profile_flag >> create_profile_flag_table >> create_evol_profile
    upload_profile_to_evol >> send_profile_success_upload
    upload_profile_to_evol >> check_device_field
    get_mask >> churn_alert
    get_mask >> create_new_partition >> save_previous_profile_in_history >> drop_old_partitions
    save_previous_profile_in_history >> create_evol_profile

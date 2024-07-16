from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.decorators import task
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator
from airflow.providers.oracle.operators.oracle import OracleOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.utils.task_group import TaskGroup

from datetime import timedelta, datetime


@task
def get_mask(**context):
	pass

@task
def check_fields(**context):
	pass


default_args = {
    'start_date':  datetime(2024, 7, 15),
    'retries': 0,
    'retry_delay': timedelta(minutes=30),
    'email_on_failure': True,
    'email': ['DSuchkov@beeline.uz'],
    'do_xcom_push': False,
    'owner': 'Denis',
}

with DAG(
        dag_id='generate_partprofile',
        default_args=default_args,
        schedule_interval='0 6 * * *',
        catchup=False,
        max_active_runs=1,
        tags=['evolution'],
        params={'load_date_%d.%m.%Y': None},
        description='''
         DAG creates and uploads to Evolution system:
         1. morning part of CVM profile.
         2. daily canceled subs.
         3. daily product catalog.
         4. save previous profile into profile_history table.
         If you want to use a specific date for profile, please use manual trigger with param "load_date_%d.%m.%Y". 
         ''',
    ) as dag:


    check_subs_flag = SFTPSensor(

    )

    get_mask = get_mask()

    with TaskGroup('create_temp_tables') as create_temp_tables:

        check_agg_snp_prepaid_flag = SFTPSensor(

        )

        create_cvm_profile_subs_tmp = OracleOperator(

        )

        create_cvm_profile_soc_pp_tmp = OracleOperator(

        )

        create_cvm_profile_age_tmp = OracleOperator(


        )

        create_gp_profile_imei_tmp = PostgresOperator(

        )

        create_profile_flag_table = PostgresOperator(

        )

        create_cvm_profile_services_tmp = OracleOperator(

        )

        create_evol_churn = PostgresOperator(

        )

        truncate_cvm_profile_recharges_tmp= OracleOperator(

        )

        truncate_cvm_profile_arpu_tmp= OracleOperator(

        )

        check_bar_cvm_flag = SFTPSensor(
    
        )

        check_agg_snp_prepaid_flag >> create_cvm_profile_subs_tmp
        create_ora_cvm_profile_imei_tmp >> create_gp_profile_imei_tmp


    create_evol_profile_test = PostgresOperator(

    )

    create_csv_profile = PostgresOperator(


    )

    check_fields = check_fields()

    profile_alert = EmailOperator(

    )

    remove_bar_cvm_flag = SSHOperator(

    )

    #-----------------------------------------
    # SAVE PROFILE IN HISTORY
    # -----------------------------------------

    create_new_partition = PostgresOperator(

    )

    save_previous_profile_in_history = PostgresOperator(

    )

    drop_old_partitions = PostgresOperator(

    )

    #-----------------------------------------

    #-----------------------------------------
    # CANCELED SUBS PREVIOUS DAY
    # -----------------------------------------
    create_csv_canceled = PostgresOperator(

    )

    upload_canceled_to_evol = SSHOperator(

    )

    #-----------------------------------------
    # PRODUCT CATALOG
    # -----------------------------------------

    run_proc_product_catalogue = OracleOperator(

    )

    create_csv_catalog = PostgresOperator(

    )

    upload_catalog_to_evol = SSHOperator(

    )
    # -----------------------------------------


(check_subs_flag >> get_mask >> create_temp_tables >> create_evol_profile_test >>
 create_csv_profile >> upload_profile_to_evol >> check_fields >> profile_alert)
upload_profile_to_evol >> remove_bar_cvm_flag
check_subs_flag >> run_proc_product_catalogue >> create_csv_catalog >> upload_catalog_to_evol
check_subs_flag >> create_csv_canceled >> upload_canceled_to_evol
get_mask >> create_new_partition >> save_previous_profile_in_history >> drop_old_partitions >> create_evol_profile_test

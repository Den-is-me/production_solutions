# product_solutions

**Airflow**
___
* The [dag_generate_and_load_data.py](dag_generate_and_load_data.py) is waiting for the flag from the DWH and generates three files from 3 in parallel for the Business platform. It uses PostgresOperator, to create a table in Greenplum and download files to Greenplum server. Then, it utilizes the SSHOperator to execute shell scripts and upload files to the final server. It also checks the data for accuracy and sends alerts.
![image](https://github.com/Den-is-me/product_solutions/assets/107809488/63f6661b-1f4f-4cdb-ac29-637b9fc804ee)


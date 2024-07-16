# production solutions

## **Airflow**
___
**The [dag_generate_and_load_data.py](dag_generate_and_load_data.py) waits for the flag from DWH and generates three files in parallel for the CVM Business platform.**

![image](https://github.com/user-attachments/assets/7b32e445-e263-430b-86df-d3ef602be230)

 
 Tools:
*  FileSensor to check the Flag.
*  BranchOperator to check validation of data.
*  PostgresOperator, OracleOperator to isert into or truncate tables and create new files.
*  SSHOperator, BashOperator to execute shell scripts to move files.
*  EmailOperator for alert.

___
**The [dag_parce_upload_csv_files_tech](dag_parce_upload_csv_files_tech.py) runs every 4 hours to parse data from 5 servers for 3 technologies (2G, 3G, 4G) in parallel. It transforms the data using Python, uploads the data into the DWH, creates KPIs using the uploaded data, and sends the KPI CSV files to an external server.**

![image](https://github.com/user-attachments/assets/d010d096-413e-419d-9c08-f45119488fcf)

 Tools:
*  [Python parser](Parser_4G.py) for extract, transform, load DATA
*  SSHOperator to run parsers.
*  BranchOperator to check validation of data.
*  PostgresOperator, OracleOperator to isert into or truncate tables and create new files.
*  SSHOperator, BashOperator to execute shell scripts to move files.
*  EmailOperator for alert.

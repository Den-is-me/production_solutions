# product_solutions

## **Airflow**
___
**The [dag_generate_and_load_data.py](dag_generate_and_load_data.py) is waiting for the flag from the DWH and generates three files from 3 in parallel for the Business platform.**
![image](https://github.com/Den-is-me/product_solutions/assets/107809488/63f6661b-1f4f-4cdb-ac29-637b9fc804ee)
 It uses:
*  FileSensor to check the Flag.
*  PostgresHook to select next date.
*  BranchOperator to decide count of running for today and select mask for files.
*  PostgresOperator, OracleOperator to create new tables and new files.
*  SSHOperator, BashOperator to execute shell scripts to move files.
*  EmailOperator for alert.


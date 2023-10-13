# product_solutions

**Airflow**
___
* The [dag_generate_and_load_data.py](dag_generate_and_load_data.py) is waiting for a flag from the DWH and generates two files in parallel for the Business platform. It uses the PostgresOperator to create a table in Greenplum and download the file to a server. Then, it utilizes the SSHOperator to execute shell scripts and move the file to the final server.
![image](https://github.com/Den-is-me/product_solutions/assets/107809488/d574cb40-6606-43d8-9836-8986a5822f91)

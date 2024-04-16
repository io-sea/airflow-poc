# Airflow-poc
PoC Airlfow Integration with IO-SEA Workflow manager. [Apache Airflow](https://airflow.apache.org/?ref_type=heads) is an open-source platform for developing, scheduling, and monitoring workflows. The core component in Airflow is Directed Acyclic Graph (DAG) – collection of all the tasks you want to run, organized in a way that reflects their relationships and dependencies. DAGs are written in Python.


## DAGs
There are two DAGs. Main one that shows it's possible to use Airflow to automate the entire pipeline for WFM - **io_sea_wfm.py**. The second DAG shows that it is also possible to upload files via Airflow - **io_sea_scp.py**. Both use the [SSH operator](https://airflow.apache.org/docs/apache-airflow-providers-ssh/stable/_api/airflow/providers/ssh/operators/ssh/index.html?ref_type=heads) that is programmed in Airflow.


### io_sea_wfm.py
In order to run this DAG, a python script - **get_status.py** - needs to be uploaded to Deep(WFM is running there). This script checks the status of the ongoing workflow and gives information whether it is possible to continue with the next step. For simplicity, the required input files - **input** - with which the DAG was tested are also attached.

Input parameters:
```
wdf_file_path - path to the yaml file required by WFM
session_name - name of session from wdf
status_timeout - what is the maximum time to wait between individual steps (sec)
step_name - name of step from wdf
skip_start - whether the session should be created or already exists
skip_clean - whether to delete or keep the session
```
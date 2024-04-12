from airflow import DAG
from airflow.models.param import Param
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

# Define your SSH connection parameters
ssh_hook = SSHHook(ssh_conn_id='IO_SEA_DEEP', cmd_timeout=None)  # specify the ID of your SSH connection created in Airflow UI

def if_zip_test(**kwargs):
    print("" + kwargs['unzip'])
    if(kwargs['unzip'] == "True"):
        return ['make_cmd_unzip']
    return ['bash_info'] 

def make_cmd_unzip(**kwargs):
    #scp -i "C:\Users\Jan Faltýnek\Desktop\IT4I\IO-SEA\Items\faltynek_deep" "sbb_test.sh.txt" faltynek1@deep.fz-juelich.de:/p/home/jusers/faltynek1/deep/wfm/tests/sbb_test
    cmd = "unzip -u -d /p/home/jusers/faltynek1/deep/wfm/tests/ /p/home/jusers/faltynek1/deep/wfm/tests/" + kwargs['destination_file_name']
    print(cmd)
    return cmd

with DAG(
    dag_id='io_sea_deep_testing',
    schedule=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={
        "source_file_path": Param("/home/user/test.zip", type="string"),
        "destination_file_name": Param("testDeep.zip", type="string"),
        "unzip": Param(False, type="boolean"),
    },
    
) as dag:
    
    
    task_bash_scp = BashOperator(
        task_id="scp",
        #depends_on_past=False,    
        bash_command="scp -i /home/user/airflow/key/faltynek_deep $source_file_path faltynek1@deep.fz-juelich.de:/p/home/jusers/faltynek1/deep/wfm/tests/$destination_file_name",
        env={'source_file_path': "{{params.source_file_path}}", 'destination_file_name': "{{params.destination_file_name}}"},
    )
    
    branch_unzip_task = BranchPythonOperator(
        task_id='branching',
        op_kwargs={'unzip': "{{params.unzip}}", },
        python_callable=if_zip_test,
        dag=dag,
    )
    
    py_task_make_cmd_unzip = PythonOperator(
        task_id="make_cmd_unzip",
        op_kwargs={'destination_file_name': "{{params.destination_file_name}}"},
        python_callable=make_cmd_unzip,
        do_xcom_push=True,
    )
    
    
    ssh_task_unzip = SSHOperator(
        task_id='ssh_unzip',
        ssh_hook=ssh_hook,
        command = "{{ task_instance.xcom_pull('make_cmd_unzip') }}",
    )
    
    task_bash_info = BashOperator(
        task_id="bash_info",  
        bash_command="echo Done",
        trigger_rule="none_failed_min_one_success",
    )

    
    task_bash_scp >> branch_unzip_task >> [py_task_make_cmd_unzip, task_bash_info]
    py_task_make_cmd_unzip >> ssh_task_unzip >> task_bash_info
    task_bash_info
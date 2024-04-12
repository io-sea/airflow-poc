from airflow import DAG
from airflow.models.param import Param
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator

from datetime import datetime, timedelta

# Define your SSH connection parameters
ssh_hook = SSHHook(ssh_conn_id='IO_SEA_DEEP', cmd_timeout=None)  # specify the ID of your SSH connection created in Airflow UI
    
def make_cmd_start(**kwargs) -> str:
    #iosea-wf start -w wdf_test.yaml -s fal_test2
    cmd = "iosea-wf start -w " + kwargs['wdf_file_path'] + " -s " + kwargs['session_name']
    print(cmd)
    return cmd
    
def make_cmd_statcontrol(**kwargs) -> str:
    #python /p/home/jusers/faltynek1/deep/wfm/get_status.py fal_test2 allocated 20
    cmd = "python /p/home/jusers/faltynek1/deep/wfm/get_status.py " + kwargs['session_name'] + " " + kwargs['type'] + " " + kwargs['timeout']
    print(cmd)
    return cmd
    
def make_cmd_runstep(**kwargs) -> str:
    #iosea-wf run -s fal_test2 -t step1
    cmd = "iosea-wf run -s " + kwargs['session_name'] + " -t " + kwargs['step_name']
    print(cmd)
    return cmd
    
def make_cmd_cleansession(**kwargs) -> str:
    #iosea-wf stop -s fal_test2
    cmd = "iosea-wf stop -s " + kwargs['session_name']
    print(cmd)
    return cmd
    
def if_start_test(**kwargs):
    print("" + kwargs['skip_start'])
    if(kwargs['skip_start'] == "True"):
        print("skip_start == True")
        return ['make_cmd_statcontrol']
    print("skip_start == False")
    return ['make_cmd_start']
    
def if_clean_test(**kwargs):
    print("" + kwargs['skip_clean'])
    if(kwargs['skip_clean'] == "True"):
        print("skip_clean == True")
        return []
    print("skip_clean == False")
    return ['make_cmd_cleansession']


with DAG(
    dag_id='io_sea_deep_v1.2',
    schedule=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={
        "wdf_file_path": Param("~/wfm/wdf_test.yaml", type="string"),
        "session_name": Param("fal_test3", type="string"),
        "status_timeout": Param("600", type="string"),
        "step_name": Param("step1", type="string"),
        "skip_start": Param(False, type="boolean"),
        "skip_clean": Param(False, type="boolean"),
    },
    
) as dag:

    branch_task = BranchPythonOperator(
        task_id='branching',
        op_kwargs={'skip_start': "{{params.skip_start}}", },
        python_callable=if_start_test,
        dag=dag,
    )
    
    branch_task_clean = BranchPythonOperator(
        task_id='branching_clean',
        op_kwargs={'skip_clean': "{{params.skip_clean}}", },
        python_callable=if_clean_test,
        dag=dag,
    )
    
    py_task_make_cmd_start = PythonOperator(
        task_id="make_cmd_start",
        op_kwargs={'session_name': "{{params.session_name}}", 'wdf_file_path': "{{params.wdf_file_path}}",},
        python_callable=make_cmd_start,
        do_xcom_push=True,
    )
    
    ssh_task_start = SSHOperator(
        task_id='ssh_start',
        ssh_hook=ssh_hook,
        command = "{{ task_instance.xcom_pull('make_cmd_start') }}",
    )
    
    py_task_make_cmd_statcontrol = PythonOperator(
        task_id="make_cmd_statcontrol",
        op_kwargs={'session_name': "{{params.session_name}}", 'type': 'allocated', 'timeout': "{{params.status_timeout}}",},
        python_callable=make_cmd_statcontrol,
        do_xcom_push=True,
        trigger_rule="none_failed_min_one_success",
    )
    
    ssh_task_stat_control = SSHOperator(
        task_id='ssh_control_status',
        ssh_hook=ssh_hook,
        command = "{{ task_instance.xcom_pull('make_cmd_statcontrol') }}",
    )
    
    py_task_make_cmd_runstep = PythonOperator(
        task_id="make_cmd_runstep",
        op_kwargs={'session_name': "{{params.session_name}}", 'step_name': "{{params.step_name}}",},
        python_callable=make_cmd_runstep,
        do_xcom_push=True,
    )
    
    ssh_task_run_step = SSHOperator(
        task_id='ssh_run_step',
        ssh_hook=ssh_hook,
        command = "{{ task_instance.xcom_pull('make_cmd_runstep') }}",
    )
    
    py_task_make_statcontrolfin = PythonOperator(
        task_id="make_cmd_statcontrolfin",
        op_kwargs={'session_name': "{{params.session_name}}", 'type': 'stopped', 'timeout': "{{params.status_timeout}}",},
        python_callable=make_cmd_statcontrol,
        do_xcom_push=True,
    )
    
    ssh_task_stat_controlfin = SSHOperator(
        task_id='ssh_control_statusfin',
        ssh_hook=ssh_hook,
        command = "{{ task_instance.xcom_pull('make_cmd_statcontrolfin') }}",
    )
    
    py_task_make_cmd_cleansession = PythonOperator(
        task_id="make_cmd_cleansession",
        op_kwargs={'session_name': "{{params.session_name}}", },
        python_callable=make_cmd_cleansession,
        do_xcom_push=True,
    )
    
    ssh_task_cleansession = SSHOperator(
        task_id='ssh_cleansession',
        ssh_hook=ssh_hook,
        command = "{{ task_instance.xcom_pull('make_cmd_cleansession') }}",
    )
        
    branch_task >> [py_task_make_cmd_start, py_task_make_cmd_statcontrol]
    py_task_make_cmd_start >> ssh_task_start >> py_task_make_cmd_statcontrol >> ssh_task_stat_control >> py_task_make_cmd_runstep >> ssh_task_run_step >> py_task_make_statcontrolfin >> ssh_task_stat_controlfin >> branch_task_clean >> [py_task_make_cmd_cleansession, ] 
    py_task_make_cmd_statcontrol >> ssh_task_stat_control >> py_task_make_cmd_runstep >> ssh_task_run_step >> py_task_make_statcontrolfin >> ssh_task_stat_controlfin >> branch_task_clean >> [py_task_make_cmd_cleansession, ]
    
    py_task_make_cmd_cleansession >> ssh_task_cleansession
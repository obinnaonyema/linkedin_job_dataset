
import airflow
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator


bash_commands = """
put file:///opt/airflow/data/benefits.csv /home/data/lab/
"""

args = {"owner": "Group3", "start_date": airflow.utils.dates.days_ago(2)}

dag = DAG(
    dag_id = "HDFS_Load_Test", default_args = args, schedule_interval=None
)

with dag:
    # task1 = SSHOperator(
    #     ssh_conn_id = 'AzureVM',
    #     task_id = 'Connect_to_VM',
    #     command = bash_commands
    # )

    run_this = BashOperator(
    task_id="run_after_loop",
    bash_command='ls -la /opt/airflow/plugins/upload/',
    )

    taskSFTP = SFTPOperator(
    task_id="test_sftp",
    ssh_conn_id="AzureVM",
    local_filepath="/opt/airflow/plugins/upload/companies.csv",
    remote_filepath="/home/data/lab/companies.csv",
    operation="put",
    create_intermediate_dirs=True
    )






# # Using BashOperator module in airflow
# 1. Connect to Azure VM via SSH
# 2. copy files from local to project folder in VM
# 3. use bash to start hadoop
# 4. use put command to move files into HDFS 
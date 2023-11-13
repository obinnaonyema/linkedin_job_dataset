
import airflow
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator

bash_commands = """
cp /opt/airflow/data/* /home/data/lab/
"""

args = {"owner": "Group3", "start_date": airflow.utils.dates.days_ago(2)}

dag = DAG(
    dag_id = "HDFS_Load_Test", default_args = args, schedule_interval=None
)

with dag:
    task1 = SSHOperator(
        ssh_conn_id = 'AzureVM',
        task_id = 'Connect_to_VM',
        command = bash_commands
    )




# # Using BashOperator module in airflow
# 1. Connect to Azure VM via SSH
# 2. copy files from local to project folder in VM
# 3. use bash to start hadoop
# 4. use put command to move files into HDFS 
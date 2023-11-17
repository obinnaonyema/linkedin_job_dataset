
import airflow
from airflow.decorators import task
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
import os

# location variables
file_source = "/opt/airflow/data/upload/"
sftp_destination = "/home/data/lab/project/"
hdfs_destination = "/user/hdoop/lab/project/"

# hdfs file copy commands
hdfs_bash_commands = f"""
sudo su hdoop <<EOF
# define path to hadoop so we can run hadoop commands in this block
export PATH=$PATH:/home/hdoop/hadoop-3.2.2/bin  
cd
cd hadoop-3.2.2/sbin
# Check if NameNode and DataNode for DFS are running
if ! jps | grep -q 'NameNode'; then
    echo "Starting DFS..."
    ./start-dfs.sh
else
    echo "DFS already running."
fi
# Check if ResourceManager and NodeManager for YARN are running
if ! jps | grep -q 'ResourceManager'; then
    echo "Starting YARN..."
    ./start-yarn.sh
else
    echo "YARN already running."
fi
# Create folder in HDFS if it doesn't already exist
hadoop fs -test -d {hdfs_destination}

if [[ $? -ne 0 ]]; then
    echo "project directory does not exist. Creating..."
    hadoop fs -mkdir /path/to/directory
else
    echo "project directory already exists."
fi
# force overwrite if exists (edit this later to move to archive after copy)
hadoop fs -put -f {sftp_destination}* {hdfs_destination}
EOF
"""

# DAG initialization variables
args = {"owner": "Group3", "start_date": airflow.utils.dates.days_ago(2)}

dag = DAG(
    dag_id = "HDFS_Load_Test", default_args = args, schedule_interval=None
)    

with dag:
    # upload files to project landing folder on VM
    # refer to doc https://airflow.apache.org/docs/apache-airflow-providers-sftp/stable/_api/airflow/providers/sftp/operators/sftp/index.html
    taskSFTP = SFTPOperator(
    task_id="load_to_vm",
    ssh_conn_id="AzureVM",
    local_filepath=[file_source+name for name in os.listdir(file_source)],
    remote_filepath=[sftp_destination+name for name in os.listdir(file_source)],
    operation="put",
    create_intermediate_dirs=True
    )

    # move files to HDFS
    store_files_in_hdfs = SSHOperator(
        ssh_conn_id="AzureVM",
        task_id="copy_to_hdfs",
        command=hdfs_bash_commands
    )    

    taskSFTP >> store_files_in_hdfs



## NOTES:
# sudo has been configured for hdoop to bypass password request
# the data folder should be loaded with the project files. In production files won't come from the airflow container anyway.
# there will be another local or remote location for file source

# # Using SSHOperator module in airflow
# 1. Connect to Azure VM via SSH
# 2. copy files from local to project folder in VM
# 3. use bash to start hadoop
# 4. use put command to move files into HDFS 

## Next
# 5. Run SQL to create or update tables in Hive using files uploaded to HDFS
# 6. Run transformation SQL/Pyspark
# 7. Save final tables in Hive
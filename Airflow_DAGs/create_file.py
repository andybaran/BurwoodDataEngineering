# Create a file in your Cloud Composer bucket and then delete it.
# Can be used with gcs_sensor DAG if the deletefile task is not used or a delay is inserted by way of using the bash operator and sleep command


from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator

args = {
    'start_date': datetime(2019, 6, 17),
    'provide_context': True,
    'owner': 'airflow',
    'depends_on_past':True
}

dag = DAG(
    dag_id='create_file',
    default_args=args,
    schedule_interval=None,
)

with dag:
    createfile = BashOperator(
        task_id="create_file",
        bash_command="touch /home/airflow/gcs/data/trigger.trg"
    )

    copytobucket = GoogleCloudStorageToGoogleCloudStorageOperator(
        task_id="copy_to_bucket",
        source_bucket="your_cloud_composer_bucket",
        source_object="data/trigger.trg",
        destination_bucket="your_cloud_composer_bucket",
        destination_object="data/sense_for_me.txt",
        google_cloud_storage_conn_id="google_cloud_storage_default",
        move_object="true"
    )

    deletefile = BashOperator(
        task_id="delete_file",
        bash_command="rm /home/airflow/gcs/data/sense_for_me.txt"
    )

    createfile >> copytobucket >> deletefile
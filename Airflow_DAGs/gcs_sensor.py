# Illustrates use of the Google Cloud Storage sensor operator
# Replace lines 27 & 28 with the URI of the storage object and your bucket name
# Look at create_file.py for a way to automate creation of the object and chain together DAGs

from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor

args = {
    'start_date': datetime(2019, 6, 17),
    'email' : ['you@your_org.org'],
    'email_on_failure': True,
    'email_on_retry': True,
    'provide_context': True,
    'owner': 'airflow',
    'depends_on_past':True
}

dag = DAG(
    dag_id='gcs_sensor',
    default_args=args,
    schedule_interval=None,
)

gcs_sensor = GoogleCloudStorageObjectSensor(
    task_id="sense",
    bucket='bucket_of_stuff',
    object='sense_for_me.txt',
    google_cloud_storage_conn_id='google_cloud_storage_default',
    soft_fail=True,
    poke_interval=5,
    timeout=15,
    mode="poke",
    dag=dag
)

start = DummyOperator(
    task_id="start",
    dag=dag
)

end = DummyOperator(
    task_id="end",
    dag=dag
)

start >> gcs_sensor >> end
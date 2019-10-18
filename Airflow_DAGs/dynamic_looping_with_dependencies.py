# Demonstrates dynamically building a list of tasks and set their dependencies.
# To keep the example simple dependentTasks (line 32) is a simple python list 
# dependentTasks could be results pulled from a text file, sql query, etc.
# While this pattern is not always best practice it is very useful in the real world and a good learning example.

from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

args = {
    'start_date': datetime.utcnow(),
    'owner': 'airflow',
}

dag = DAG(
    dag_id='dynamic_looping_with_dependencies',
    default_args=args,
    schedule_interval=None,
)

dummy_start = DummyOperator(
    task_id='start',
    dag=dag
)

dummy_end = DummyOperator(
    task_id='end',
    dag=dag
)

dependentTasks = [5,11,3,1]
dependentTasks.sort()

last_task = None


for task in dependentTasks:

    middle_task = DummyOperator(
        task_id='middle_task_{}'.format(task),
        dag=dag
    )

    if last_task:
        last_task >> middle_task
    if not last_task:
        dummy_start >> middle_task

    last_task = middle_task

middle_task >> dummy_end
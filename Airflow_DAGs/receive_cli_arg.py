from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

args = {
    'start_date': datetime(2019, 6, 17),
    'email' : ['you@yourorg.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'provide_context': True,
    'owner': 'airflow',
    'depends_on_past':True
}

dag = DAG(
    dag_id='receiving_cli_arg',
    default_args=args,
    schedule_interval=None,
)

def run_this_func(ds, **kwargs):
    print("Remotely received value of {} for key=message".
          format(kwargs['dag_run'].conf['message']))


run_this = PythonOperator(
    task_id='run_this',
    provide_context=True,
    python_callable=run_this_func,
    dag=dag,
)

run_this
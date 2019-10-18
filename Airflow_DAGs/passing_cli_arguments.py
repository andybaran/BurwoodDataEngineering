# This example illustrates the ability to accept and act on arguments recieved from a CLI based start of a DAG
# The argument(s) are passed to "receive_cli" using the TriggerDAG operator.

from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

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
    dag_id='passing_cli_arg',
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

# You can also access the DagRun object in templates
bash_task = BashOperator(
    task_id="bash_task",
    bash_command='echo "Here is the message: '
                 '{{ dag_run.conf["message"] if dag_run else "" }}" ',
    dag=dag,
)

def run_receiving_cli_arg(context,dag_run_obj):
    #message = kwargs['dag_run'].conf['message']
    #message = context['params']['message']
    dag_run_obj.payload = { 'message' : context['dag_run'].conf['message'] }
    return dag_run_obj

trigger_receive = TriggerDagRunOperator(
    task_id='trigger_receive_cli_arg',
    trigger_dag_id='receiving_cli_arg',
    python_callable=run_receiving_cli_arg,
    #params={'message': "{{ dag_run.conf['message'] }}"},
    #templates_dict={'message': "{{ dag_run.conf['message'] }}"},
    dag=dag
)

run_this >> bash_task >> trigger_receive
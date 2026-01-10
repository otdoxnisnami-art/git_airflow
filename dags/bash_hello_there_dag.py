from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


default_args = {
    'owner': 'snakhmad',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 9),
    'retries': 0
}

dag = DAG('bash_hello_there_dag',
    default_args=default_args,
    schedule='00 20 * * *',
    catchup=False
)

run_this = BashOperator(
    task_id='run_after_loop',
    bash_command='echo 1',
    dag=dag
)
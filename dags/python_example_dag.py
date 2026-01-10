from airflow.providers.standard.operators.bash import BashOperator
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import pathlib

default_args = {
    'owner': 'snakhmad',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 9),
    'retries': 0
}

dag = DAG('calculate_example',
    default_args=default_args,
    catchup=False,
    schedule='00 20 * * *'
)

def hello():
    return print('Hello there!')

def sum_int():
    return print(2+2)

t1 = PythonOperator(
    task_id='calculate_task',
    python_callable=hello,
    dag=dag
)

t2 = PythonOperator(
    task_id='calculate_task_2',
    python_callable=sum_int,
    dag=dag
)

t3 = BashOperator(
    task_id='calculate_task_3',
    bash_command='pyhon hello_there_script.py',
    dag=dag
)

t1 >> t2 >> t3

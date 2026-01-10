from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pathlib


default_args = {
    'owner': 'snakhmad',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 9),
    'retries': 0
}

dag = DAG('python_hello_there_dag',
    default_args=default_args,
    schedule='00 20 * * *',
    catchup=False
)


def hello():
    return print('Hello there!')

def my_name():
    return print('I am Suleiman')

def sys_path():
    return print(pathlib.Path(__file__).parent.absolute())


t1 = PythonOperator(
    task_id='print_hello_there',
    python_callable=hello,
    dag=dag
)


t2 = PythonOperator(
    task_id='print_my_name',
    python_callable=my_name,
    dag=dag
)


t3 = PythonOperator(
    task_id='sys_path',
    python_callable=sys_path,
    dag=dag
)


t1 >> t2 >> t3
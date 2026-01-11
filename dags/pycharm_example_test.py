from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'snakhmad',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 10),
    'retries': 0
}

dag = DAG('pycharm_dag_lect',
    default_args=default_args,
    catchup=False,
    schedule='00 20 * * *'
)

def create_report_nba():
    import requests
    import pandas as pd

    year = 1999
    download_url = "https:''raw.githubusercontent.com/fivethirtyeight/data/master/nba-elo/nbaallelo.csv"
    target_csv_path = "nba_all_elo.csv"
    response = requests.get(download_url)
    response.raise_for_status() #check that the request was succesful
    with open(target_csv_path, 'wb') as f:
        f.write(response.content)
    print("Download is ready!")
    nba = pd.read_csv("nba_all_elo.csv")
    nba = nba.groupby('year_id').game_id.nunique().reset_index()
    nba['rolling_games'] = nba['game_id'].rolling(5).mean()
    #посчитаем разницу между скользящим средним и реальным кол-вом игр
    nba = int(round(nba[nba['year_id'] == year]['rolling_games'] - nba[nba['year_id'] == year]['game_id']))
    #теперь сделаем текстовое описание найденной метрики
    new_report = f'Отклонение в {year} году равно {nba}'
    #запишем в текстовый файл
    text_file = open('example_report.txt', 'w')
    text_file.write(new_report)
    text_file.close()
    print("Report is written!")


t1 = PythonOperator(
    task_id='write_report',
    python_callable=create_report_nba,
    dag=dag
)

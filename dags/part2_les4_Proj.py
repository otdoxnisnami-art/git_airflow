from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'snakhmad',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 10),
    'retries': 0
}

dag = DAG('dag_snakhmad_miniproject',
    default_args=default_args,
    catchup=False,
    schedule='00 20 * * *'
)

def send_report_to_vk():
    import pandas as pd
    import numpy as np
    import vk_api
    import random

    # Чтение данных
    path = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vR-ti6Su94955DZ4Tky8EbwifpgZf_dTjpBdiVH0Ukhsq94jZdqoHuUytZsFZKfwpXEUCKRFteJRc9P/pub?gid=889004448&single=true&output=csv'
    ads = pd.read_csv(path, parse_dates=[0])

    ads_views = ads[ads['event'] == 'view'].groupby(['date', 'ad_id']).count().reset_index()[['date', 'ad_id', 'event']]
    ads_views.columns = ['date', 'ad_id', 'views']
    ads_clicks = ads[ads['event'] == 'click'].groupby(['date', 'ad_id']).count().reset_index()[['date', 'ad_id', 'event']]
    ads_clicks.columns = ['date', 'ad_id', 'clicks']
    
    ads_ctr = pd.merge(ads_views, ads_clicks, on = ['date', 'ad_id'])
    ads_ctr['CTR'] = ads_ctr['clicks'] / ads_ctr['views']
    
    ads['ad_action_cost'] = ads['ad_cost'] / 1000
    ads_ctr['money'] = ads_ctr['views'] * ads.ad_action_cost.unique()[0] 
    print('Данные считаны')

    # Метрики
    money_0204 = float(ads_ctr[ads_ctr['date'] == '2019-04-02']['money'])
    views_0204 = float(ads_ctr[ads_ctr['date'] == '2019-04-02']['views'])
    clicks_0204 = float(ads_ctr[ads_ctr['date'] == '2019-04-02']['clicks'])
    CTR_0204 = float(ads_ctr[ads_ctr['date'] == '2019-04-02']['CTR'])

    money_0104 = float(ads_ctr[ads_ctr['date'] == '2019-04-01']['money'])
    views_0104 = float(ads_ctr[ads_ctr['date'] == '2019-04-01']['views'])
    clicks_0104 = float(ads_ctr[ads_ctr['date'] == '2019-04-01']['clicks'])
    CTR_0104 = float(ads_ctr[ads_ctr['date'] == '2019-04-01']['CTR'])

    diff_money = round((money_0204 - money_0104) / money_0104 * 100)
    diff_views = round((views_0204 - views_0104) / views_0104 * 100)
    diff_clicks = round((clicks_0204 - clicks_0104) / clicks_0104 * 100)
    diff_CTR = round((CTR_0204 - CTR_0104) / CTR_0104 * 100)
    print('Метрики посчитаны')
    
    # Создание отчета
    message_vk = f'''Отчёт по объявлению 121288 за 2 апреля\n
    Траты: {money_0204} ({diff_money}%)
    Показы: {views_0204} ({diff_views}%)
    Клики: {clicks_0204} ({diff_clicks}%)
    CTR: {CTR_0204} ({diff_CTR}%)'''
    print('Отчет создан')

    # Отправка ВК
    app_token = 'vk1.a.POKtOMfqU92cjD7AUS1o4rVCr3M2GsbLfajEu94DsVkkXhwimQic4_bmuQtjvOmS-dfpvvGmu3hIeX9wkTDyjGc5kEEWndVNA3PuyW6MEuZeIl1UJWT2Q2fBbLqjCXU4bPlhhnL5SHrw7oHc0zD-F-kkRCCfdbr3S-MeSz9HvBKhWZi6-kncPixKNbo7T_L6tG_SWBLaY_Pu6_DpDoJYAg'
    chat_id = 1
    my_id = 2000000048
    vk_session = vk_api.VkApi(token=app_token)
    vk = vk_session.get_api()
    
    vk.messages.send(
        chat_id=chat_id,
        random_id=np.random.randint(0, 2 ** 31),
        message=message_vk
    )
    print('Отчет отправлен')

    
    
    t1 = PythonOperator(
    task_id='ads_report',
    python_callable=send_report_to_vk,
    dag=dag
)
    

    

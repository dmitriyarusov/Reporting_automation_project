from datetime import datetime, timedelta
import telegram
import requests
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20230220',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }

my_token = '6116887711:AAHlmLXMeSAB6040WfJ5QiziVZGH60CG6fM' 
bot = telegram.Bot(token = my_token) # получаем доступ

chat_id = -802518328

default_args = {
    'owner': 'd-jarusov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 18),
}

schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def kc_report_djarusov():

    @task()
    def find_metrics():
        query = """
            SELECT toDate(time) as date, COUNT(DISTINCT user_id) as DAU,
            countIf(user_id, action = 'view') as views, 
            countIf(user_id, action = 'like') as likes,  
            countIf(user_id, action = 'like') / countIf(user_id, action = 'view') as CTR 
            FROM simulator_20230220.feed_actions
            WHERE toDate(time) BETWEEN  today() - 7 AND yesterday()
            GROUP BY toDate(time) """
        df_metrics = ph.read_clickhouse(query=query, connection=connection)
        return df_metrics
    
    @task()
    def send_metrics(df_metrics):
        df_yesterday_metrics = df_metrics.query('date == date.max()').reset_index(drop=True)
        print(df_yesterday_metrics)
        date = df_yesterday_metrics['date'].loc[0].strftime('%d-%m-%Y')
        DAU = df_yesterday_metrics['DAU'].loc[0] 
        views = df_yesterday_metrics['views'].loc[0]
        likes = df_yesterday_metrics['likes'].loc[0]
        CTR = round(df_yesterday_metrics['CTR'].loc[0] , 3)
              
        message = f"""Метрики за вчерашний день:
Дата: {date}
DAU: {DAU}
Просмотры: {views}
Лайки: {likes}
CTR: {CTR} """
                    
        bot.sendMessage(chat_id=chat_id, text=message)
    
        fig, axes = plt.subplots(2, 2, figsize=(20, 10))
        for ax, col in zip(axes.flatten(), ['DAU', 'views', 'likes', 'CTR']):
            sns.lineplot(data=df_metrics, x='date', y=col, ax=ax)
            ax.set(title=f'{col}')
        for ax in axes.flatten():
            ax.set_ylabel('')  
            
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'last_week_metrics.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
  
    df_metrics = find_metrics()
    send_metrics(df_metrics)
    
kc_report_djarusov = kc_report_djarusov()

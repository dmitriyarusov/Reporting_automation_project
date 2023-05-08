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

chat_id =  -802518328

default_args = {
    'owner': 'd-jarusov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 18),
}

schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def kc_full_report_djarusov():

    @task()
    def feed_metrics():
        query1 = """
            SELECT toDate(time) as date, COUNT(DISTINCT user_id) as DAU,
            countIf(user_id, action = 'view') as views, 
            countIf(user_id, action = 'like') as likes,  
            countIf(user_id, action = 'like') / countIf(user_id, action = 'view') as CTR 
            FROM simulator_20230220.feed_actions
            WHERE toDate(time) BETWEEN  today() - 7 AND yesterday()
            GROUP BY toDate(time) """
        feed_metrics_df = ph.read_clickhouse(query=query1, connection=connection)
        return feed_metrics_df
    
    @task()
    def feed_metrics():
        query1 = """
            SELECT toDate(time) as date, 
            countIf(user_id, action = 'view') as views, 
            countIf(user_id, action = 'like') as likes,  
            countIf(user_id, action = 'like') / countIf(user_id, action = 'view') as CTR 
            FROM simulator_20230220.feed_actions
            WHERE toDate(time) BETWEEN  today() - 7 AND yesterday()
            GROUP BY toDate(time) """
        feed_metrics_df = ph.read_clickhouse(query=query1, connection=connection)
        return feed_metrics_df
    
    
    @task()
    def all_users():
        query2 = """ SELECT COUNT(DISTINCT user_id) as all_users
                       FROM
                         (SELECT DISTINCT user_id
                          FROM simulator_20230220.feed_actions) t1
                       FULL JOIN
                         (SELECT DISTINCT user_id
                          FROM simulator_20230220.message_actions) t2 USING user_id """  
        all_users_df = ph.read_clickhouse(query=query2, connection=connection)
        return all_users_df

    @task()
    def active_users():
        query3 = """ SELECT date,
                            count(user_id) AS active_users
                        FROM
                          (SELECT user_id , date
                           FROM
                             (SELECT DISTINCT user_id,
                                              toDate(time) AS date
                              FROM simulator_20230220.feed_actions
                              WHERE toDate(time) BETWEEN  today() - 7 AND yesterday()) t1
                           JOIN
                             (SELECT DISTINCT user_id,
                                              toDate(time) AS date
                              FROM simulator_20230220.message_actions
                              WHERE toDate(time) BETWEEN  today() - 7 AND yesterday()) t2 
                              ON t1.user_id = t2.user_id AND t1.date = t2.date
                              ) AS virtual_table
                        GROUP BY date
                        ORDER BY date
                         """  
        active_users_df = ph.read_clickhouse(query=query3, connection=connection)
        return active_users_df
    
    @task
    def dau():
        query4 = """select date, COUNT(DISTINCT user_id) DAU
                    from (
                        select user_id, toDate(time) date
                        from simulator_20230220.feed_actions
                        UNION ALL
                        select user_id, toDate(time) date
                        from simulator_20230220.message_actions)
                    WHERE date BETWEEN  today() - 7 AND yesterday()
                    group by date"""
        dau_df = ph.read_clickhouse(query=query4, connection=connection)
        return dau_df

    @task
    def messages():
        query5 = """SELECT toDate(time) as date, COUNT(user_id) messages
                    FROM simulator_20230220.message_actions
                    WHERE toDate(time) BETWEEN  today() - 7 AND yesterday()
                    GROUP BY toDate(time)
                    ORDER BY date"""
        messages_df = ph.read_clickhouse(query=query5, connection=connection)
        return messages_df  

    @task
    def users():
        query6 =  """SELECT date, feed_users, message_users
                FROM
                (SELECT toDate(time) as date, COUNT(DISTINCT user_id) feed_users FROM simulator_20230220.feed_actions
                WHERE toDate(time) = yesterday() AND user_id NOT IN 
                (SELECT DISTINCT user_id FROM simulator_20230220.message_actions WHERE toDate(time) = yesterday())
                GROUP BY toDate(time) as date) t1
            JOIN 
                (SELECT toDate(time) as date, COUNT(DISTINCT user_id) message_users FROM simulator_20230220.message_actions
                WHERE toDate(time) = yesterday() AND user_id NOT IN 
                (SELECT DISTINCT user_id FROM simulator_20230220.feed_actions WHERE toDate(time) = yesterday())
                GROUP BY toDate(time) as date) t2
            USING(date)"""
        users_df = ph.read_clickhouse(query=query6, connection=connection)
        return users_df       
        
   
    
    @task()
    def send_metrics(feed_metrics_df, all_users_df, active_users_df, dau_df, messages_df, users_df):
        yesterday_feed_metrics_df = feed_metrics_df.query('date == date.max()').reset_index(drop=True)
        yesterday_dau_df = dau_df.query('date == date.max()').reset_index(drop=True)
        yesterday_messages_df = messages_df.query('date == date.max()').reset_index(drop=True)
        date = yesterday_feed_metrics_df['date'].loc[0].strftime('%d-%m-%Y')        
        views = yesterday_feed_metrics_df['views'].loc[0]
        likes = yesterday_feed_metrics_df['likes'].loc[0]
        CTR = round(yesterday_feed_metrics_df['CTR'].loc[0] , 3)
        DAU = yesterday_dau_df['DAU'].loc[0]
        yesterday_messages = yesterday_messages_df['messages'].loc[0]
        all_users = all_users_df['all_users'].loc[0] 
        feed_users = users_df['feed_users'].loc[0]
        message_users = users_df['message_users'].loc[0]
        
        message = f"""Количество пользователей сервисом за все время: {all_users}
        
Метрики за вчерашний день:
Дата: {date}
Просмотры: {views}
Лайки: {likes}
CTR: {CTR}
DAU (всего сервиса): {DAU}
Сообщений: {yesterday_messages}
Пользователей только лентой: {feed_users}
Пользователей только мессенджером: {message_users}"""
                    
        bot.sendMessage(chat_id=chat_id, text=message)
           
        fig, ax = plt.subplots(2, 2, figsize=(20, 10))
        
        date_1 = datetime.strftime(datetime.now() - timedelta(7), '%d-%m-%Y')
        date_2 = datetime.strftime(datetime.now() - timedelta(1), '%d-%m-%Y') 
        
        fig.suptitle(f'Основные метрики c {date_1} по {date_2}', fontsize = 25)
        
        sns.lineplot(ax=ax[0,0], data = dau_df, x='date', y='DAU')
        ax[0,0].set_title('DAU')
        sns.lineplot(ax=ax[0,1], data = feed_metrics_df, x='date', y='CTR')
        ax[0,1].set_title('CTR')
        sns.lineplot(ax=ax[1,0], data = active_users_df, x='date', y='active_users')
        ax[1,0].set_title('Активная аудитория')
        sns.lineplot(ax=ax[1,1], data = messages_df, x='date', y='messages')
        ax[1,1].set_title('Количество отправленных сообщений')
        # ax[1,1].set_yticklabels(list(messages_df['messages'].apply(lambda x: str(x) )))
        ax[1,1].get_yaxis().get_major_formatter().set_useOffset(False)
        
        for ax in ax.flatten():
            ax.set_ylabel('') 
            ax.set_xlabel('Дата')
            
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'last_week_metrics.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
  

    feed_metrics_df = feed_metrics()
    all_users_df = all_users()
    active_users_df = active_users()
    dau_df = dau()
    messages_df = messages()
    users_df = users()
    send_metrics(feed_metrics_df, all_users_df, active_users_df, dau_df, messages_df, users_df)
    
kc_full_report_djarusov = kc_full_report_djarusov()

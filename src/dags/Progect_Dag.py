import requests
import json
import datetime
from datetime import date, timedelta
import time #Для корректного времени
import psycopg2 #Для postgresql
from psycopg2.extras import Json

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator


#Текущая дата для выгрузки
today = date.today()
current_date = str(date.today())
current_date_string = current_date + ' 00:00:00'

#Вчерашняя дата для выгрузки
yesterday_date = date.today() - timedelta(1)
yesterday_date_string = str(yesterday_date) + ' 00:00:00'

#Заголовки для API
headers = {
'X-Nickname': 'GrigorovichAP',
'X-Cohort': '5',
'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'
}

file_api = 'couriers' #deliveries\couriers

offset = 1
limit = 10000

DWH = "dbname='de' port=15432 user=jovyan host=localhost password=jovyan"

 #####################залить Stage и ODS курьеров

def load_couriers (file_api = file_api, offset = offset, limit = limit, DWH = DWH, current_date_string = current_date_string):


    #Очистить таблицу
    with psycopg2.connect(DWH) as conn:
        cur = conn.cursor()
        query = f"TRUNCATE TABLE stg.api_{file_api};"
        print(query)
        cur.execute(query)
        conn.commit()

    #Заполнить таблицу
    while True:
        url = f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/{file_api}?from={yesterday_date_string}&to={current_date_string}&limit={limit}&offset={offset}'
        r = requests.get(url,headers=headers)
        d = r.json()
        data = str(d)
        offset+=1

        if len(d) > 1:
            with psycopg2.connect(DWH) as conn:
                cur = conn.cursor()
                query = f"INSERT INTO stg.api_{file_api}(content) VALUES ('{json.dumps(d[0], ensure_ascii=False)}'::json)"
                print(query)
                cur.execute(
                    query
                        )

                conn.commit()
        if not d:

            break

    #Залть в stage таблицу с чеками и типами
    with psycopg2.connect(DWH) as conn:
        cur = conn.cursor()
        query = f"CALL Load_stg_{file_api}()"
        print(query)
        cur.execute(query)
        conn.commit()
        
    #Залть в ODS таблицу 'мерджом'     
    with psycopg2.connect(DWH) as conn:
        cur = conn.cursor()
        query = f"CALL Load_dds_{file_api}()"
        print(query)
        cur.execute(query)
        conn.commit()

    return 200

file_api = 'deliveries' #deliveries\couriers
    
#Объявляем даг
dag = DAG(
    dag_id='load_new',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60)
)


file_api = 'deliveries' #deliveries\couriers                              
                                        
 #####################залить Stage доставки                                   
                                        
def load_deliveries (file_api = file_api, offset = offset, limit = limit, DWH = DWH, current_date_string = current_date_string):


    #Очистить таблицу
    with psycopg2.connect(DWH) as conn:
        cur = conn.cursor()
        query = f"TRUNCATE TABLE stg.api_{file_api};"
        print(query)
        cur.execute(query)
        conn.commit()

    #Заполнить таблицу
    while True:
        url = f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/{file_api}?from={yesterday_date_string}&to={current_date_string}&limit={limit}&offset={offset}'
        r = requests.get(url,headers=headers)
        d = r.json()
        data = str(d)
        offset+=1

        if len(d) > 1:
            with psycopg2.connect(DWH) as conn:
                cur = conn.cursor()
                query = f"INSERT INTO stg.api_{file_api}(content) VALUES ('{json.dumps(d[0], ensure_ascii=False)}'::json)"
                print(query)
                cur.execute(
                    query
                        )

                conn.commit()
        if not d:

            break

    #Залть в таблицу с чеками и типами
    with psycopg2.connect(DWH) as conn:
        cur = conn.cursor()
        query = f"CALL Load_stg_{file_api}()"
        print(query)
        cur.execute(query)
        conn.commit()
        
    #Залть в ODS таблицу 'мерджом'     
    with psycopg2.connect(DWH) as conn:
        cur = conn.cursor()
        query = f"CALL Load_dds_{file_api}()"
        print(query)
        cur.execute(query)
        conn.commit()        

    #Залить подготовить заказы  (тут т.к. делвется из доставок)
    with psycopg2.connect(DWH) as conn:
        cur = conn.cursor()
        query = f"CALL Load_dds_orders ()"
        print(query)
        cur.execute(query)
        conn.commit()     

    return 200

    

 #####################залить подготовить заказы                                   
                                        
def Load_dm_courier_ledger (file_api = file_api, offset = offset, limit = limit, DWH = DWH, current_date_string = current_date_string):

    #Очистить таблицу
    with psycopg2.connect(DWH) as conn:
        cur = conn.cursor()
        query = f"CALL Load_dm_courier_ledger ()"
        print(query)
        cur.execute(query)
        conn.commit()

    return 200


t_load_couriers = PythonOperator(task_id='load_couriers',
                                        python_callable=load_couriers,
                                        op_kwargs= {'headers':headers},
                                        dag=dag)


t_load_deliveries = PythonOperator(task_id='load_deliveries',
                                        python_callable=load_deliveries,
                                        op_kwargs= {'headers':headers},
                                        dag=dag)



t_Load_dm_courier_ledger = PythonOperator(task_id='Load_dm_courier_ledger',
                                        python_callable=Load_dm_courier_ledger,
                                        op_kwargs= {'headers':headers},
                                        dag=dag)




# Курьеры и доставки, заказы в STG и ODS        #Перезалить DM таблицу за месяц и год загружаемого дня             
[t_load_couriers, t_load_deliveries] >> t_Load_dm_courier_ledger
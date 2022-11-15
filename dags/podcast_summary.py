import pendulum
from datetime import datetime
import requests
import xmltodict
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

PODCAST_URL = "https://www.marketplace.org/feed/podcast/marketplace/"
LANDING_DATABASE='land.podcast_data'
POSTGRES_CONN_ID='postgres_default'
SEARCH_='https'
def get_episode():
            data=requests.get(PODCAST_URL)
            feed=xmltodict.parse(data.text)
            response=feed["rss"]["channel"]["item"]
            return str(response).replace("'",'"')

def clean_data(raw_data):
    clean_=str(raw_data).replace('[(','').replace(', )]','').replace("'",'').replace(',)]','')  
    return clean_

def retrieve_data(**context):
    PG_Hook=PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    Hook=PG_Hook.get_conn()
    cursor = Hook.cursor()
    cursor.execute(f'''select podcast_data from {LANDING_DATABASE}''')
    response_=cursor.fetchall()
    response_=str(response_).replace('[(','').replace(', )]','').replace("'",'').replace(',)]','')  
    context['task_instance'].xcom_push(key='response_', value=response_)
    return response_


def transform(**context):
    data=context['task_instance'].xcom_pull(task_ids='get_data',key='response_')
    date = str(context["execution_date"])
    dag_id = "podcast_summary"
    task_id = "transform_data"
    episodes=str(data).replace('null','"None"').replace('style="text-align: left"','style=text-align: left')
    eval_episodes=eval(episodes)
    PG_Hook=PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    new_episode=[]
    for epi_ in eval_episodes:
        if SEARCH_ in epi_['SEARCH_']:
            filename=epi_['SEARCH_'].split('/')[-1]+'.mp3'
            print('filename',filename)
            new_episode.append([epi_["SEARCH_"],epi_["title"],epi_["pubDate"],epi_["description"],filename,date,dag_id,task_id])
    PG_Hook.insert_rows(table="stage.podcast_data",rows=new_episode,target_fields=["SEARCH_","title","pubDate","description","filename","execution_date","dag_id","task_id",])
    print('!!!!!!!!!!!!!!!!Done Hurra!!!!!!!!!!!!')


with DAG(
    dag_id='podcast_summary',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022,5,31),
    catchup=False   
    ) as dag:


    Start=EmptyOperator(task_id='Start')
    End=EmptyOperator(task_id='End')

    Create_Table=PostgresOperator(
            task_id="Create_Table",
            sql="""
                CREATE SCHEMA IF NOT EXISTS land;
                CREATE SCHEMA IF NOT EXISTS Stage;
                CREATE TABLE IF NOT EXISTS land.podcast_data(
                postcast_id SERIAL PRIMARY KEY,
                podcast_data TEXT,
                created_at TEXT
                );
                CREATE TABLE IF NOT EXISTS stage.podcast_data(
                id SERIAL PRIMARY KEY,
                api_transform TEXT,
                SEARCH_ TEXT,
                title TEXT,
                pubDate TEXT,
                description TEXT,
                filename TEXT,
                execution_date TEXT,
                dag_id TEXT,
                task_id TEXT
                );
            """
            )
    data=get_episode()
    Load_Date=PostgresOperator(
            task_id="Load_Data",
            sql="""
                TRUNCATE TABLE land.podcast_data;
                INSERT INTO land.podcast_data (podcast_data,created_at)
                VALUES (%(data_)s,%(date)s);
                """,
                parameters={'data_':str(data),'date':str(datetime.now())}

    )

    get_data=PythonOperator(task_id='get_data',python_callable=retrieve_data,provide_context=True)

    transform_data=PythonOperator(task_id='transform_data',python_callable=transform)                        

    Start>>Create_Table>>Load_Date>>get_data>>transform_data>>End







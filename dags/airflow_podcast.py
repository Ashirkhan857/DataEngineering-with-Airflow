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
STAGING_DATABASE='stage.podcast_data'
POSTGRES_CONN_ID='postgres_default'
SEARCH_='https'
DAG_ID = "PODCAST_SUMMARY"
LANDING_TASK_ID='LOAD_DATA'

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
    clean_response=clean_data(response_) 
    context['task_instance'].xcom_push(key='response_', value=clean_response)
    return response_


def transform(**context):
    data=context['task_instance'].xcom_pull(task_ids='READ_DATA_FROM_LAND_TABLE',key='response_')
    date = str(context["execution_date"])
    task_id = "transform_data"
    episodes=str(data).replace('null','"None"').replace('style="text-align: left"','style=text-align: left')
    eval_episodes=eval(episodes)
    PG_Hook=PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    new_episode=[]
    for epi_ in eval_episodes:
        if SEARCH_ in epi_['link']:
            filename=epi_['link'].split('/')[-1]+'.mp3'
            print('filename',filename)
            new_episode.append([epi_["link"],epi_["title"],epi_["pubDate"],epi_["description"],filename,date,DAG_ID,task_id])
    PG_Hook.insert_rows(table=STAGING_DATABASE,rows=new_episode,target_fields=["link","title","pubDate","description","filename","execution_date","dag_id","task_id",])


with DAG(
    dag_id='podcast_summary',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022,5,31),
    catchup=False   
    ) as dag:


    Start=EmptyOperator(task_id='START')
    End=EmptyOperator(task_id='END')

    Create_Table=PostgresOperator(
            task_id="CREATE_TABLE",
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
                link TEXT,
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
    Load_Data=PostgresOperator(
            task_id="LOAD_DATA_IN_LAND_TABLE",
            sql="""
                TRUNCATE TABLE land.podcast_data;
                INSERT INTO land.podcast_data (podcast_data,created_at,dag_id,task_id)
                VALUES (%(data_)s,%(date)s,%(dag_id)s,%(task_id)s);
                """,
                parameters={'data_':str(data),'date':str(datetime.now()),'dag_id':DAG_ID ,'task_id':LANDING_TASK_ID}

    )

    Read_Data=PythonOperator(task_id='READ_DATA_FROM_LAND_TABLE',python_callable=retrieve_data,provide_context=True)

    Transform_Data=PythonOperator(task_id='TRANFORM_DATA_AND_LOAD_INTO_STAGE',python_callable=transform)                        

    Start>>Create_Table>>Load_Data>>Read_Data>>Transform_Data>>End







from airflow import DAG
from airflow.decorators import task, dag
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
import etl_dota

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    f'dota_dag',
    default_args=default_args,
    description=f'Dota DAG',
    schedule_interval=None,
    catchup=False
)

def extract_heroes():
    etl_dota.extract('all_heroes_id')

def extract_meta():
    etl_dota.extract('current_pro_meta')

def transform_heroes():
    etl_dota.transform_heroes('all_heroes_id')

def transform_meta():
    etl_dota.transform_meta('current_pro_meta')

def load_heroes():
    etl_dota.load('all_heroes_id')

def load_meta():
    etl_dota.load('current_pro_meta')

extract_heroes = PythonOperator(
    task_id='extract_heroes',
    python_callable=extract_heroes,
    dag=dag,
)

extract_meta = PythonOperator(
    task_id='extract_meta', 
    python_callable=extract_meta,
    dag=dag,
)

transform_heroes = PythonOperator(
    task_id='transform_heroes',
    python_callable=transform_heroes,
    dag=dag,
)

transform_meta = PythonOperator(
    task_id='transform_meta',
    python_callable=transform_meta,
    dag=dag,
)

load_heroes = PythonOperator(
    task_id='load_heroes',
    python_callable=load_heroes,
    dag=dag,
)

load_meta = PythonOperator(
    task_id='load_meta',
    python_callable=load_meta,
    dag=dag,
)

extract_heroes >> extract_meta >> transform_heroes >> transform_meta >> load_heroes >> load_meta
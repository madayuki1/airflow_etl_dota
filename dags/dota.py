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

def transform_heroes():
    etl_dota.transform('all_heroes_id')

def load_heroes():
    etl_dota.load('all_heroes_id')

fetch_task = PythonOperator(
    task_id='extract_heroes',
    python_callable=extract_heroes,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_heroes',
    python_callable=transform_heroes,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_heroes',
    python_callable=load_heroes,
    dag=dag,
)

fetch_task >> transform_task >> load_task
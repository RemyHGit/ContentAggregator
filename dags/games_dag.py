from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))

from games_mongodb_script import sync_all_games_threaded

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sync_igdb_games',
    default_args=default_args,
    description='Synch games from IGDB to a local MongoDB',
    schedule_interval=timedelta(days=1),  # Exécution quotidienne
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['igdb', 'games', 'mongodb'],
)

def sync_games_task(**context):
    sync_all_games_threaded(parts=4)

sync_games = PythonOperator(
    task_id='sync_igdb_games',
    python_callable=sync_games_task,
    dag=dag,
)

sync_games


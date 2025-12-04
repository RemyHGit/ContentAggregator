from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))

from movies_mongodb_script import sync_movies_file_add_db_threaded

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sync_tmdb_movies',
    default_args=default_args,
    description='Synch movies from TMDB to a local MongoDB',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['tmdb', 'movies', 'mongodb'],
)

def sync_movies_task(**context):
    """Tâche pour synchroniser les films TMDB"""
    sync_movies_file_add_db_threaded(parts=10, only_new=True)

sync_movies = PythonOperator(
    task_id='sync_tmdb_movies',
    python_callable=sync_movies_task,
    dag=dag,
)

sync_movies


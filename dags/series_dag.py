from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Ajouter le chemin des plugins pour les imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))

from series_mongodb_script import sync_series_file_add_db_threaded

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sync_tmdb_series',
    default_args=default_args,
    description='Synchronise les séries depuis TMDB vers MongoDB',
    schedule_interval=timedelta(days=1),  # Exécution quotidienne
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['tmdb', 'series', 'mongodb'],
)

def sync_series_task(**context):
    """Tâche pour synchroniser les séries TMDB"""
    sync_series_file_add_db_threaded(parts=10, only_new=True)

sync_series = PythonOperator(
    task_id='sync_tmdb_series',
    python_callable=sync_series_task,
    dag=dag,
)

sync_series


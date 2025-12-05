from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))

from books_mongodb_script import sync_books_threaded

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sync_google_books',
    default_args=default_args,
    description='Synchronise les livres depuis Google Books vers MongoDB',
    schedule_interval=timedelta(days=1),  # Exécution quotidienne
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['google', 'books', 'mongodb'],
)

def sync_books_task(**context):
    """Tâche pour synchroniser les livres Google Books"""
    sync_books_threaded(parts=10, max_books_per_query=500)

sync_books = PythonOperator(
    task_id='sync_google_books',
    python_callable=sync_books_task,
    dag=dag,
)

sync_books


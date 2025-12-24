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
    'sync_openlibrary_books',
    default_args=default_args,
    description='Sync books from Open Library to a dockerized MongoDB',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['openlibrary', 'books', 'mongodb'],
    schedule_interval=timedelta(days=7),
)

def sync_books_task(**context):
    """task to sync books from Open Library to a dockerized MongoDB"""
    sync_books_threaded(dump_type="editions", parts=4, auto_download=True, only_new=True)

sync_books = PythonOperator(
    task_id='sync_openlibrary_books',
    python_callable=sync_books_task,
    dag=dag,
)

sync_books


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))

from games_mongodb_script import sync_all_games_threaded
from movies_mongodb_script import sync_movies_file_add_db_threaded
from series_mongodb_script import sync_series_file_add_db_threaded
from music_mongodb_script import sync_music_threaded
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
    'sync_all_sources',
    default_args=default_args,
    description='Sync all contents (games, movies, series, musics, books) to a dockerized MongoDB',
    schedule_interval=timedelta(days=7),  # Exécution quotidienne
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['sync', 'all', 'mongodb'],
)

def sync_games_task(**context):
    """Task to sync games from IGDB"""
    sync_all_games_threaded(parts=4)

def sync_movies_task(**context):
    """Task to sync movies from TMDB"""
    sync_movies_file_add_db_threaded(parts=10, only_new=True)

def sync_series_task(**context):
    """Task to sync series from TMDB"""
    sync_series_file_add_db_threaded(parts=10, only_new=True)

def sync_music_task(**context):
    """Task to sync music from MusicBrainz"""
    sync_music_threaded(dump_date="LATEST", parts=4)

def sync_books_task(**context):
    """Task to sync books from Open Library"""
    sync_books_threaded(dump_type="editions", parts=4, auto_download=True, only_new=True)

# tasks
sync_games = PythonOperator(
    task_id='sync_igdb_games',
    python_callable=sync_games_task,
    dag=dag,
)

sync_movies = PythonOperator(
    task_id='sync_tmdb_movies',
    python_callable=sync_movies_task,
    dag=dag,
)

sync_series = PythonOperator(
    task_id='sync_tmdb_series',
    python_callable=sync_series_task,
    dag=dag,
)

sync_music = PythonOperator(
    task_id='sync_musicbrainz_music',
    python_callable=sync_music_task,
    dag=dag,
)

sync_books = PythonOperator(
    task_id='sync_openlibrary_books',
    python_callable=sync_books_task,
    dag=dag,
)
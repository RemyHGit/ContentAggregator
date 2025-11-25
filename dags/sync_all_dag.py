from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Ajouter le chemin des plugins pour les imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))

from games_mongodb_script import sync_all_games_threaded
from movies_mongodb_script import sync_movies_file_add_db_threaded
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
    'sync_all_sources',
    default_args=default_args,
    description='Synchronise tous les contenus (jeux, films, séries) vers MongoDB',
    schedule_interval=timedelta(days=1),  # Exécution quotidienne
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['sync', 'all', 'mongodb'],
)

def sync_games_task(**context):
    """Tâche pour synchroniser les jeux IGDB"""
    sync_all_games_threaded(parts=4)

def sync_movies_task(**context):
    """Tâche pour synchroniser les films TMDB"""
    sync_movies_file_add_db_threaded(parts=10, only_new=True)

def sync_series_task(**context):
    """Tâche pour synchroniser les séries TMDB"""
    sync_series_file_add_db_threaded(parts=10, only_new=True)

# Création des tâches
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

# Les tâches s'exécutent en parallèle (pas de dépendances)
# Si tu veux qu'elles s'exécutent séquentiellement, décommente les lignes suivantes:
# sync_games >> sync_movies >> sync_series


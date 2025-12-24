from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))

from music_mongodb_script import sync_music_threaded

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sync_brainz_music',
    default_args=default_args,
    description='Sync musics from MusicBrainz to a dockerized MongoDB',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['musicbrainz', 'music', 'mongodb'],
    schedule_interval=timedelta(days=1),

)

def sync_music_task(**context):
    """task to sync musics from MusicBrainz to a dockerized MongoDB"""
    # read release-groups from dumps, filter for Album/EP/Single only, and update MongoDB with threading
    sync_music_threaded(dump_date="LATEST", parts=4, only_new=True)

sync_music = PythonOperator(
    task_id='sync_musicbrainz_music',
    python_callable=sync_music_task,
    dag=dag,
)

sync_music


import sys
print(sys.path)
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import json

from datetime import datetime, timedelta
from airflow import DAG
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def getaudiobooks(ti):

    client_credentials_manager = SpotifyClientCredentials(client_id = "b37d9bb15aba42ddb31be145e7c9245b", client_secret = "2b6bf657f5ea49f985e86d72bdf5c862")

    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)

    audiobook_link = "https://api.spotify.com/v1/audiobooks/7iHfbu1YPACw6oZPAFJtqe"

    print(audiobook_link)

    audiobook_data = sp._get(audiobook_link)

    with open("audiobook_data.json", "w") as json_file:
        json.dump(audiobook_data, json_file, indent=4)

    print("Audiobook data saved as audiobook_data.json")

    chapter_list = []
    for row in audiobook_data['chapters']['items']:
        id = row['id']
        description = row['description']
        chapter_number = row['chapter_number']
        duration = row['duration_ms']
        images = []
        for image in row['images']:
            images.append({'height': image['height'], 'width': image['width'], 'url': image['url']})
        name = row['name']
        release_date = row['release_date']
        release_date_precision = row['release_date_precision']
        available_markets = row['available_markets']

        chapter_element = {
            'id': id,
            'name': name,
            'description': description,
            'chapter_number': chapter_number,
            'duration_ms': duration,
            'images': images,
            'release_date': release_date,
            'release_date_precision': release_date_precision,
            'available_markets': available_markets
            }
        chapter_list.append(chapter_element)
        
    chapterlist_df = pd.DataFrame(chapter_list)

   
    book = chapterlist_df.to_dict('records')
    print(book)
    ti.xcom_push(key='audiobook_data', value=chapterlist_df.to_dict('records'))


def insert_book_data_into_postgres(ti):
    book_data = ti.xcom_pull(key='audiobook_data', task_ids='fetch_audiobook_data')
    if not book_data:
        raise ValueError("No book data found")

    postgres_hook = PostgresHook(postgres_conn_id='audibooks_connection')
    insert_query = """
    INSERT INTO audiobooks (id, description, chapter_number, duration_ms, images, release_date, release_date_precision, available_markets)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    for book in book_data:
        postgres_hook.run(
            insert_query, 
            parameters=(
                book['id'], 
                book['description'], 
                book['chapter_number'], 
                book['duration_ms'], 
                json.dumps(book['images']), 
                book['release_date'], 
                book['release_date_precision'], 
                book['available_markets']  
            )
        )

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_and_store_spotify_audiobooks',
    default_args=default_args,
    description='A simple DAG to fetch audiobook data from Spotify API and store it in Postgres',
    schedule_interval=timedelta(days=1),
)

fetch_book_data_task = PythonOperator(
    task_id='fetch_audiobook_data',
    python_callable=getaudiobooks,
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='audibooks_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS audiobooks (
    id TEXT PRIMARY KEY, 
    description TEXT, 
    chapter_number INTEGER, 
    duration_ms INTEGER, 
    images JSONB, 
    release_date DATE, 
    release_date_precision VARCHAR(10), 
    available_markets TEXT[]
);
    """,
    dag=dag,
)


insert_book_data_task = PythonOperator(
    task_id='insert_book_data',
    python_callable=insert_book_data_into_postgres,
    dag=dag,
)

#dependencies

fetch_book_data_task >> create_table_task >> insert_book_data_task

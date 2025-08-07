import json
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

default_args = {
    "owner": "imrang",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 4)
}

def _fetch_spotify_data(**kwargs):
    client_id = Variable.get("spotify_client_id")
    client_secret = Variable.get("spotify_client_secret")
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret))

    playlist_link = "https://open.spotify.com/playlist/54ZA9LXFvvFujmOVWXpHga"
    playlist_URI = playlist_link.split("/")[-1].split("?")[0]

    spotify_data = sp.playlist_tracks(playlist_URI)
    
    filename = "spotify_raw_" + datetime.now().strftime("%Y%m%d%H%M%S") + ".json"
    kwargs['ti'].xcom_push(key="spotify_filename", value=filename)
    kwargs['ti'].xcom_push(key="spotify_data", value=json.dumps(spotify_data))
    print("Fetched Spotify data and pushed to XCom.")

def _upload_to_s3(**context):
    ti = context['ti']
    filename = ti.xcom_pull(task_ids='fetch_spotify_data', key='spotify_filename')
    data = ti.xcom_pull(task_ids='fetch_spotify_data', key='spotify_data')

    s3 = S3Hook(aws_conn_id='aws_s3_spotify')
    s3_key = f"raw_data/to_processed/{filename}"
    s3.load_string(
        string_data=data,
        key=s3_key,
        bucket_name="spotify-etl-project-imrang",
        replace=True
    )
    print(f"Uploaded to S3: {s3_key}")

def _read_data_from_S3(**kwargs):
    s3_hook = S3Hook(aws_conn_id="aws_s3_spotify")
    bucket_name = "spotify-etl-project-imrang"
    prefix = "raw_data/to_processed/"

    keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)

    spotify_data = []
    for key in keys:
        if key.endswith(".json"):
            data = s3_hook.read_key(key, bucket_name)
            spotify_data.append(json.loads(data))
    print(spotify_data)
    kwargs['ti'].xcom_push(key="spotify_data", value=spotify_data)

def _process_album(**context):
    ti = context['ti']
    spotify_data = ti.xcom_pull(task_ids="read_data_from_S3", key="spotify_data")
    
    album_list = []
    for data in spotify_data:
        for row in data['items']:
            album = row['track']['album']
            album_element = {
                'album_id': album['id'],
                'name': album['name'],
                'release_date': album['release_date'],
                'total_tracks': album['total_tracks'],
                'url': album['external_urls']['spotify']
            }
            album_list.append(album_element)

    album_df = pd.DataFrame.from_dict(album_list)
    album_df = album_df.drop_duplicates(subset=['album_id'])
    album_df['release_date'] = pd.to_datetime(album_df['release_date'],errors='coerce')
    album_buffer = StringIO()
    album_df.to_csv(album_buffer, index=False)
    album_content = album_buffer.getvalue()
    ti.xcom_push(key="album_content", value=album_content)

def _process_artist(**context):
    ti = context['ti']
    spotify_data = ti.xcom_pull(task_ids="read_data_from_S3", key="spotify_data")

    artist_list = []
    for data in spotify_data:
        for row in data['items']:
            for artist in row['track']['artists']:
                artist_dict = {
                    'artist_id': artist['id'],
                    'artist_name': artist['name'],
                    'external_url': artist['href']
                }
                artist_list.append(artist_dict)

    artist_df = pd.DataFrame.from_dict(artist_list)
    artist_df = artist_df.drop_duplicates(subset=['artist_id'])
    artist_buffer = StringIO()
    artist_df.to_csv(artist_buffer, index=False)
    artist_content = artist_buffer.getvalue()
    ti.xcom_push(key='artist_content', value=artist_content)

def _process_songs(**context):
    ti = context['ti']
    spotify_data = ti.xcom_pull(task_ids="read_data_from_S3", key="spotify_data")

    song_list = []
    for data in spotify_data:
        for row in data['items']:
            track = row['track']
            song_element = {
                'song_id': track['id'],
                'song_name': track['name'],
                'duration_ms': track['duration_ms'],
                'url': track['external_urls']['spotify'],
                'popularity': track['popularity'],
                'song_added': row['added_at'],
                'album_id': track['album']['id'],
                'artist_id': track['album']['artists'][0]['id']
            }
            song_list.append(song_element)

    song_df = pd.DataFrame.from_dict(song_list)
    song_df['song_added'] = pd.to_datetime(song_df['song_added'])
    song_buffer = StringIO()
    song_df.to_csv(song_buffer, index=False)
    song_content = song_buffer.getvalue()
    ti.xcom_push(key='song_content', value=song_content)



def _move_proccessed_data(**context):
    s3_hook=S3Hook(aws_conn_id="aws_s3_spotify")

    bucket_name="spotify-etl-project-imrang"
    prefix="raw_data/to_processed/"
    target_prefix="raw_data/processed/"

    keys=s3_hook.list_keys(bucket_name=bucket_name,prefix=prefix)
    for key in keys:
        if key.endswith(".json"):
            new_key=key.replace(prefix,target_prefix)
            s3_hook.copy_object(
                source_bucket_key=key,
                dest_bucket_key=new_key ,
                source_bucket_name=bucket_name,
                dest_bucket_name=bucket_name
                
            )
        s3_hook.delete_objects(bucket=bucket_name, keys=[key])

            

# DAG definition
with DAG(
    dag_id="spotify_etl_dag",
    default_args=default_args,
    description="ETL Process for Spotify Data",
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_spotify_data",
        python_callable=_fetch_spotify_data,
        provide_context=True
    )

    upload_to_s3 = PythonOperator(
        task_id="upload_raw_to_s3",
        python_callable=_upload_to_s3,
        provide_context=True
    )

    read_data_from_s3 = PythonOperator(
        task_id="read_data_from_S3",
        python_callable=_read_data_from_S3,
        provide_context=True
    )

    process_album = PythonOperator(
        task_id="process_album",
        python_callable=_process_album,
        provide_context=True
    )

    store_album_to_s3 = S3CreateObjectOperator(
        task_id='store_album_to_s3',
        aws_conn_id="aws_s3_spotify",
        s3_bucket="spotify-etl-project-imrang",
        s3_key="transformed_data/album_data/album_transformed_{{ ts_nodash }}.csv",
        data="{{ ti.xcom_pull(task_ids='process_album', key='album_content') }}",
        replace=True
    )

    process_artist = PythonOperator(
        task_id="process_artist",
        python_callable=_process_artist,
        provide_context=True
    )

    store_artist_to_s3 = S3CreateObjectOperator(
        task_id='store_artist_to_s3',
        aws_conn_id="aws_s3_spotify",
        s3_bucket="spotify-etl-project-imrang",
        s3_key="transformed_data/artists_data/artists_transformed_{{ ts_nodash }}.csv",
        data="{{ ti.xcom_pull(task_ids='process_artist', key='artist_content') }}",
        replace=True
    )

    process_songs = PythonOperator(
        task_id="process_songs",
        python_callable=_process_songs,
        provide_context=True
    )

    
    store_songs_to_s3 = S3CreateObjectOperator(
        task_id='store_songs_to_s3',
        aws_conn_id="aws_s3_spotify",
        s3_bucket="spotify-etl-project-imrang",
        s3_key="transformed_data/songs_data/songs_transformed_{{ ts_nodash }}.csv",
        data="{{ ti.xcom_pull(task_ids='process_songs', key='songs_content') }}",
        replace=True
    )

    move_proccessed_data=PythonOperator(
        task_id="move_processed_data",
        python_callable=_move_proccessed_data,
        provide_context=True ,
        dag=dag,
    )

    # Task dependencies
   # Task dependencies
fetch_data >> upload_to_s3 >> read_data_from_s3
read_data_from_s3 >> [process_album, process_artist, process_songs]
process_album >> store_album_to_s3
process_artist >> store_artist_to_s3 
process_songs >> store_songs_to_s3
store_album_to_s3 >> move_proccessed_data
store_artist_to_s3 >> move_proccessed_data
store_songs_to_s3 >> move_proccessed_data


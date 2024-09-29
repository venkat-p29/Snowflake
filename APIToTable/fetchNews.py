import requests
import datetime
import polars as pl
import os
from google.cloud import storage


def upload_to_gcs(bucket_name, destination_blob_name, source_file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")


def fetch_news_data():
    # Top business headlines in the US right now
    api_url = r'https://newsapi.org/v2/top-headlines?country=us&category=business&apiKey='

    response = requests.get(api_url)
    articles = response.json()['articles']

    df = pl.DataFrame(articles)
    df = df.fill_null('').with_columns(
        pl.col('content').str.slice(0, 200)
    )
    df = df.select(pl.exclude('source'))

    current_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    filename = f'run_{current_time}.parquet'
    print(df)
    
    # Check and print the current working directory
    print("Current Working Directory: ", os.getcwd())

    # Write DataFrame to Parquet file
    df.write_parquet(filename)

    # Upload to GCS
    bucket_name = 'gcs-snowflake-projects'
    destination_blob_name = f'news_data_analysis/parquet_files/{filename}'
    upload_to_gcs(bucket_name, destination_blob_name, filename)

    # Remove local file after upload
    os.remove(filename)
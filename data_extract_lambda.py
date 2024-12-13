import json
import os
import requests
import boto3
from datetime import datetime

def lambda_handler(event, context):
    
    api_key = os.environ.get("rapidapi_key")
    api_host = os.environ.get("rapidapi_host")
    URL = "https://imdb-top-100-movies.p.rapidapi.com/"
    response = requests.get(URL, headers={
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": api_host
    })
    movies_data = response.json()
    # print(movies_data)
    filename = 'imdb_movies_raw_' + str(datetime.now()) + '.json'
    
    client = boto3.client('s3')
    try:
        client.put_object(
            Bucket='imdb-top-movies-pipeline',
            Key='raw_data/to_be_processed/' + filename,
            Body=json.dumps(movies_data)
            )
    except Exception as e:
        print(e)
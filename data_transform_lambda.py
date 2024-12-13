import json
import boto3
import pandas as pd
from io import StringIO
from datetime import datetime


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = "imdb-top-movies-pipeline"
    Key = "raw_data/to_be_processed/"
    
    response = s3.list_objects(Bucket=Bucket, Prefix=Key)
    if 'Contents' not in response:
        return {'statusCode': 404, 'body': 'No files found in source'}
        
    movies_data = []
    file_keys = [file['Key'] for file in response['Contents'] if file['Key'].endswith('.json')]
    for file_key in file_keys:
       response = s3.get_object(Bucket=Bucket, Key=file_key)
       content = response['Body']
       json_object = json.loads(content.read())
       
       movies_data.append({'data': json_object, 'key': file_key})
           
           
    for movie_file in movies_data:
        data = movie_file['data']
        file_key = movie_file['key']
        movies_df = pd.DataFrame(data)

        # Data Transformations
        
        # Extract required fields
        movies_df = movies_df[['rank', 'title', 'description', 'image', 'genre', 'rating', 'id', 'year', 'imdbid', 'imdb_link']]
        
        # Rename fields
        movies_df.rename({
            'image': 'image_url'
        }, inplace=True)
        
        
        # Add year category
        bins = [1900, 1980, 1990, 2000, 2010, 2020]
        labels = ['Before 1980', '1980-1990', '1990-1999', '2000-2009', '2010-2020']
        movies_df['year_category'] = pd.cut(movies_df['year'], bins=bins, labels=labels)
        
        # Flatten genre
        movies_df['genre'] = movies_df['genre'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
        
        # Export transformed data
        transformed_file_key = "transformed_data/movies_data/movies_transformed_" + str(datetime.now()) + ".csv"
        csv_buffer = StringIO()
        movies_df.to_csv(csv_buffer, index=False, sep='\t', quoting=1)
        
        s3.put_object(
            Bucket=Bucket, 
            Key=transformed_file_key,
            Body=csv_buffer.getvalue()
        )

    # Copy to processed data and delete processed data from source to avoid re-processing same files
    s3_resource = boto3.resource('s3')
    for key in file_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': Key
        }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split('/')[-1])
        s3_resource.Object(Bucket, key).delete()
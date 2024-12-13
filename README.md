# IMDB Top 100 Movies Recommendation Pipeline

## Project Overview

- A Data Engineering Project that automates the ETL process for top 100 movies data from IMDB API using AWS services.
- The movies data is captured on a periodic basis and cleaned, transformed into a suitable format for analysis using Athena. 


## Project features**
**Data Extraction**
- Fetches the top 100 movies daily from IMDB API
- Stores the raw data in S3 bucket in JSON format

**Data Transformation**
- Triggered automatically after raw data upload in S3 
- Applies the needed transformations to the data like cleaning, adding columns to enrich existing data
- Saves transformed data to CSV file 

**Data Analysis**
- Uses AWS Glue crawler to catalog the transformed data from specified data source
- Enables querying of data for insights using AWS Athena, eg: top rated movies in the past year, year wise trends, genres etc. 

**Technologies Used**
- Python libraries: boto3, pandas, json
- AWS services: Lambda, S3, Glue, Athena
- Data formats: CSV, JSON

**Requirements**
- Python 3.8+
- AWS account
- Pandas and Jupyter Notebook (for analysis)
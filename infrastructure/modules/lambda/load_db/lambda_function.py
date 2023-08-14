####################################################################
#                         IMPORT LIBRARIES                         #
####################################################################

import os
import psycopg2
import awswrangler as wr
from sqlalchemy import create_engine
import urllib.parse
import json


####################################################################
#                     CUSTOM EXCEPTION CLASSES                     #
####################################################################

class S3ReadError(Exception):
    """Exception raised when there's an error reading from S3."""
    pass

class DatabaseWriteError(Exception):
    """Exception raised when there's an error writing to the database."""
    pass


####################################################################
#                          GLOBAL VARIABLES                        #
####################################################################

# Get environment variables
DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ['DB_PORT']
DB_NAME = os.environ['DB_NAME']
DB_USERNAME = os.environ['DB_USERNAME']
DB_PASSWORD = os.environ['DB_PASSWORD']

# Create connection string
CONN_STR = f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# Database table name and schema
DB_SCHEMA = 'public'
DB_TABLE  = 'nba_stats'


####################################################################
#                            MAIN FUNCTION                         #
####################################################################

def lambda_handler(event, context):
    """
    Lambda function handler to read Parquet data from S3 and write to a PostgreSQL database.

    Args:
        event (dict): Event data from S3 passed to the Lambda function.
        context (object): Lambda context object.

    Returns:
        None
    """
    
    print("Received event: " + json.dumps(event, indent=2) + '\n')
    
    # Get S3 key from event
    s3_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    try:
        # Read Parquet file from S3
        df = wr.s3.read_parquet(path=f's3://nba-mvp-pipeline/{s3_key}')
    except Exception as e:
        raise S3ReadError(f"Error reading from S3: {e}")
    else:
        print("Read from S3 successfully!")
        print(df.head())

    print("Writing to database...")
    
    try:
        # Write to PostgreSQL database
        df.to_sql(
            name=DB_TABLE,
            schema=DB_SCHEMA,
            con=create_engine(CONN_STR),
            if_exists='append',
            index=False
        )
    except Exception as e:
        raise DatabaseWriteError(f"Error writing to database: {e}")
    else:
        print("Wrote to database successfully!")
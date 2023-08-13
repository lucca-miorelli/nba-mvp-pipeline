import os
import psycopg2
import awswrangler as wr
from sqlalchemy import create_engine
import urllib.parse
import json

# rds settings
DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ['DB_PORT']
DB_NAME = os.environ['DB_NAME']
DB_USERNAME = os.environ['DB_USERNAME']
DB_PASSWORD = os.environ['DB_PASSWORD']


def lambda_handler(event, context):
    
    print("Received event: " + json.dumps(event, indent=2) + '\n')
    
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    try:
        df = wr.s3.read_parquet(path=f's3://nba-mvp-pipeline/{key}')
    except Exception as e:
        print(e)
    else:
        print(df.head())

    # connect to Postgresql database
    con = psycopg2.connect(
        user=DB_USERNAME,
        host=DB_HOST,
        database=DB_NAME,
        port=DB_PORT,
        password=DB_PASSWORD
    )
    
    # create cursor
    cur = con.cursor()
    
    # execute query
    cur.execute("SELECT version();")
    
    # fetch results
    record = cur.fetchone()
    print("You are connected to - ", record,"\n")
    
    # close connection
    cur.close()
    con.close()
    
    try:
        print("Writing to database...")
        conn_str = f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        
        df.to_sql('nba_stats', con=create_engine(conn_str), if_exists='append', index=False, schema='public')
            
        print("Done!")
        
    except Exception as e:
        print(f"This is the exception: {e}")

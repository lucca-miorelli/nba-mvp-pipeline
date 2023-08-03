import os
import awswrangler as wr
from sqlalchemy import create_engine

# rds settings
db_host = os.environ['DB_HOST']
db_port = os.environ['DB_PORT']
db_name = os.environ['DB_NAME']
db_username = os.environ['DB_USERNAME']
db_password = os.environ['DB_PASSWORD']

conn_str = f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'

def lambda_handler(event, context):
    
    # read parquet file from s3 with wr
    df = wr.s3.read_parquet(path='s3://nba-mvp-pipeline/data/raw/players/2023_07_27.parquet')
    
    # Set up PostgreSQL connection
    with create_engine(conn_str) as engine:
        # Upload data to PostgreSQL table
        df.to_sql('nba_stats', engine, if_exists='append', index=False, schema='public')

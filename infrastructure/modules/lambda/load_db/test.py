import awswrangler as wr
from sqlalchemy import create_engine
from configparser import ConfigParser
import pg8000
import os

config = ConfigParser()
config.read('credentials.conf')

# rds settings
DB_HOST=config['POSTGRES']['HOST']
DB_PORT=config['POSTGRES']['PORT']
DB_NAME=config['POSTGRES']['DATABASE']
DB_USERNAME=config['POSTGRES']['USER']
DB_PASSWORD=config['POSTGRES']['PASSWORD']


def df_to_sql(data_df):
    """
    Transfer data from DataFrame to PostgreSQL table
    """
    
    # get envirtonment variables related to PostgreSQL
    DB_HOST=config['POSTGRES']['HOST']
    DB_PORT=config['POSTGRES']['PORT']
    DB_NAME=config['POSTGRES']['DATABASE']
    DB_USERNAME=config['POSTGRES']['USER']
    DB_PASSWORD=config['POSTGRES']['PASSWORD']

    # connect to Postgresql database
    con = pg8000.connect(
        user=DB_USERNAME,
        host=DB_HOST,
        database=DB_NAME,
        port=DB_PORT,
        password=DB_PASSWORD
    )
    # transfer data from DataFrame to PostgreSQL table
    wr.postgresql.to_sql(
        df=data_df,
        table="nba_stats",
        schema="public",
        con=con
    )

def lambda_handler(event, context):
    
    print("Beginning")
    # read parquet file from s3 with wr
    df = wr.s3.read_parquet(path='s3://nba-mvp-pipeline/data/raw/players/2023_07_27.parquet')
    print(df.head())
    print(df.shape)
        

    try:
        # engine = create_engine(f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        # Upload data to PostgreSQL table
        # df.to_sql('nba_stats', engine, if_exists='append', index=False, schema='public')
        # engine.dispose()
        df_to_sql(df)
    except Exception as e:
        print(e)
        print("Error in connection")



if __name__ == "__main__":
    lambda_handler(None, None)
from prefect import flow, task
from prefect_sqlalchemy.credentials import DatabaseCredentials
import pandas as pd
from prefect.filesystems import S3
import io
from datetime import datetime

# Custom exception classes
class DBWriteError(Exception):
    """Exception raised when there's an error writing to the database."""
    pass

# Constants for database and S3
CURRENT_DAY = datetime.now().strftime("%Y_%m_%d")
DB_TABLE = 'nba_stats'
DB_SCHEMA = 'public'
S3_BLOCK = S3.load("nba-mvp-pipeline")
DB_BLOCK = DatabaseCredentials.load('lk-rds-credentials')

def flow_run_name_generator():
    """
    Generates a flow run name for the Prefect flow.
    
    Returns:
        str: The flow run name in the format "IngestDB-YYYY_MM_DD.parquet"
    """
    return 'IngestDB-' + CURRENT_DAY + '.parquet'

@task
def read_raw_data():
    print(f"Reading `data/raw/players/{CURRENT_DAY}.parquet` from S3...")
    # Read Parquet data from S3
    pq_bytes = S3_BLOCK.read_path(f"data/raw/players/{CURRENT_DAY}.parquet")
    pq_file = io.BytesIO(pq_bytes)
    df = pd.read_parquet(pq_file)
    return df

@task
def load_data(df):
    print("Loading data into the database...")
    # Write DataFrame to PostgreSQL database
    try:
        df.to_sql(
            name=DB_TABLE,
            schema=DB_SCHEMA,
            con=DB_BLOCK.get_engine(),
            if_exists='append',
            index=False
        )
        print("Data successfully loaded into the database.")
    except Exception as e:
        raise DBWriteError(f"Error writing to database: {e}")

@flow(name="IngestDB", flow_run_name=flow_run_name_generator, log_prints=True)
def ingest_data():
    print("Starting data ingestion...")
    # Read raw data from S3
    df = read_raw_data()
    
    # Load data into database
    load_data(df)
    print("Data ingestion completed.")

if __name__ == "__main__":
    # Run the flow
    ingest_data()

from prefect import flow
from prefect_sqlalchemy.credentials import DatabaseCredentials
import pandas as pd
from prefect.filesystems import S3
import io

s3_block = S3.load("nba-mvp-pipeline")
db_block = DatabaseCredentials.load('lk-rds-credentials')

QUERY = """
    SELECT
        *
    FROM
        nba_stats ns
    WHERE
        "Player" = 'LeBron James'
    ;
"""

df = pd.read_sql(sql=QUERY, con=db_block.get_engine())
print(df[["Player", "Tm", "snapshot_date"]])

byt = s3_block.read_path("data/raw/players/2023_08_24.parquet")
pq_file = io.BytesIO(byt)
df = pd.read_parquet(pq_file)
print(df.query("Player == 'LeBron James'")[["Player", "Tm", "snapshot_date"]])
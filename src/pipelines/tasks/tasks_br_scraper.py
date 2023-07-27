from BRScraper import nba
from prefect import task
import pandas as pd
import awswrangler as wr


@task(
    name="Get Stats",
    description="Get stats from basketball-reference.com",
    tags=["NBA", "Basketball-Reference", "Stats", "Extraction"],
)
def get_stats(season:str=None, info:str=None)->pd.DataFrame:
    
    df = nba.get_stats(season=season, info=info)

    print(f"Season: {season}\nInfo: {info}\n")
    print(df.head())

    return df

@task(
    name="Merge DataFrames",
    description="Merge totals, per_game, and advanced DataFrames into one.",
    tags=["NBA", "Basketball-Reference", "Stats", "Transformation"],
)
def merge_dfs(df1:pd.DataFrame, df2:pd.DataFrame, df3:pd.DataFrame)->pd.DataFrame:

    df = df1.merge(df2, on='Player')
    df = df.merge(df3, on='Player')

    print(df.head())

    return df

@task(

)
def add_date_column(df:pd.DataFrame, snapshot_date:str)->pd.DataFrame:
    
    df['snapshot_date'] = snapshot_date

    print(df.head())

    return df


@task(
    name="Test Players Uniqueness",
    description="Test keystring in each dataframe to ensure uniqueness.",
    tags=["NBA", "Basketball-Reference", "Stats", "Test", "Data Quality"],
)
def test_players_uniqueness(stats_t:list[str], stats_a:list[str])->None:

    set_t = set(stats_a)
    set_a = set(stats_t)

    print(set_t == set_a)
    print(set_t.difference(set_a)) 
    print(set_a.difference(set_t))


@task(
    name="Ingest Data",
    description="Save data to S3 bucket as parquet.",
    tags=["NBA", "Basketball-Reference", "Stats", "Ingestion"],
)
def load_data(df:pd.DataFrame, bucket_name:str, current_day:str)->None:

    wr.s3.to_parquet(
        df=df
        ,path=f"s3://{bucket_name}/data/raw/players/{current_day}.parquet"
    )

    print("Data saved to S3 bucket.")

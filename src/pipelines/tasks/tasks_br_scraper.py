#########################################################
#                IMPORT LIBRARIES                       #
#########################################################

from BRScraper import nba
from prefect import task
import pandas as pd
import awswrangler as wr
from functools import reduce
from typing import List


#########################################################
#                   Get Player Stats                    #
#########################################################

@task(
    name="Get Stats",
    description="Get stats from basketball-reference.com",
    tags=["NBA", "Basketball-Reference", "Stats", "Extraction"],
)
def get_stats(season: str = "2022-23", info: str = "totals") -> pd.DataFrame:
    """
    Get player statistics from basketball-reference.com.
    
    Args:
        season (str): The NBA season.
        info (str): The type of statistics.

    Returns:
        pd.DataFrame: A DataFrame containing player statistics.
    """

    # Get player statistics
    df = nba.get_stats(season=season, info=info)

    # Drop unnecessary columns based on selected info
    columns_to_drop = ['Pos', 'Age', 'G', 'Season']
    if info == "per_game":
        columns_to_drop.append('GS')
    elif info == "totals":
        columns_to_drop.append('MP')
    df = df.drop(columns=columns_to_drop)

    # Rename columns based on selected info
    if info == "per_game":
        columns_to_exclude = ["Player", "Tm"]
        new_column_suffix = "_per_game"
    elif info == "totals":
        columns_to_exclude = ["Player", "Tm"]
        new_column_suffix = "_totals"
    else:  # For "advanced" info
        columns_to_exclude = ["Player", "Tm", "Pos", "Age", "G", "MP", "Season"]
        new_column_suffix = "_advanced"

    # Rename columns
    df = df.rename(columns={i: f"{i}{new_column_suffix}" for i in df.columns if i not in columns_to_exclude})
    
    # Log information
    print(f"Processing {info} stats...")
    print(f"Season: {season}\nInfo: {info}\nShape: {df.shape}\nColumns: {list(df.columns)}\nHead:\n{df.head()}")

    return df


#########################################################
#              Check Players and Shapes                 #
#########################################################

@task(
    name="Data Quality Check Players and Duplicates",
    description="Check player uniqueness and duplicates.",
    tags=["NBA", "Basketball-Reference", "Stats", "Test", "Data Quality"],
)
def check_players_and_duplicates(dataframes:List[pd.DataFrame]) -> None:
    
    # Check player uniqueness and DataFrame shapes
    players_sets = [set(df['Player']) for df in dataframes]

    if players_sets[0] == players_sets[1] == players_sets[2]:
        print("All players are the same in each DataFrame.")
    else:
        raise ValueError("Players are not the same in each DataFrame.")
    
    # Logging information    
    print(f"\nDataFrame shape:\ndf1: {dataframes[0].shape}\ndf2: {dataframes[1].shape}\ndf3: {dataframes[2].shape}\n")
    print(f"DataFrame Unique Players:\ndf1: {len(players_sets[0])}\ndf2: {len(players_sets[1])}\ndf3: {len(players_sets[2])}\n")

    # Check for duplicates
    for i, df in enumerate(dataframes, start=1):
        if df.duplicated(subset=["Player", "Tm"]).any():
            raise ValueError(f"df{i} has duplicates.")
        else:
            print(f"df{i} has no duplicates.")

#########################################################
#                Merge DataFrames                       #
#########################################################

@task(
    name="Merge DataFrames",
    description="Merge totals, per_game, and advanced DataFrames into one.",
    tags=["NBA", "Basketball-Reference", "Stats", "Transformation"],
)
def merge_dfs(dataframes: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Merge player statistics DataFrames into one.

    Args:
        dataframes (List[pd.DataFrame]): List of DataFrames to be merged.

    Returns:
        pd.DataFrame: A merged DataFrame.
    """
    # Merge DataFrames on Player and Tm
    df = reduce(lambda left, right: pd.merge(left, right, on=["Player", "Tm"]), dataframes)

    # Logging information
    print(f"Shape: {df.shape}\nColumns: {list(df.columns)}\nHead:\n{df.head()}")

    return df


#########################################################
#             Add Snapshot Date Column                  #
#########################################################

@task
def add_date_column(df: pd.DataFrame, snapshot_date: str) -> pd.DataFrame:
    """
    Add a snapshot date column to the DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame.
        snapshot_date (str): The snapshot date.

    Returns:
        pd.DataFrame: The DataFrame with the added snapshot date column.
    """
    # Add snapshot date column
    df['snapshot_date'] = snapshot_date

    # Logging information
    print(f"Added snapshot_date column with value {snapshot_date}.")

    return df
    

#########################################################
#            Ingest Data into S3 Bucket                 #
#########################################################

@task(
    name="Ingest Data",
    description="Save data to S3 bucket as parquet.",
    tags=["NBA", "Basketball-Reference", "Stats", "Ingestion"],
)
def load_data(df: pd.DataFrame, bucket_name: str, current_day: str) -> None:
    """
    Save DataFrame to S3 bucket as parquet.

    Args:
        df (pd.DataFrame): The DataFrame to be saved.
        bucket_name (str): The S3 bucket name.
        current_day (str): The current day for the parquet file.

    Returns:
        None
    """
    # Construct the S3 path for saving parquet file
    s3_path = f"s3://{bucket_name}/data/raw/players/{current_day}.parquet"

    wr.s3.to_parquet(df=df, path=s3_path)

    # Logging information
    print("Data saved to S3 bucket.")

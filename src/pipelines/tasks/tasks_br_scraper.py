#########################################################
#                IMPORT LIBRARIES                       #
#########################################################

from BRScraper import nba
from prefect import task
import pandas as pd
import awswrangler as wr


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
        new_column_suffix = "_per_game"
    elif info == "totals":
        new_column_suffix = "_totals"
    else:  # For "advanced" info
        columns_to_exclude = ["Player", "Tm", "Pos", "Age", "G", "MP", "Season"]
        df = df.rename(columns={i: f"{i}_advanced" for i in df.columns if i not in columns_to_exclude})
    
    # Log information
    print(f"Processing {info} stats...")
    print(f"Season: {season}\nInfo: {info}\nShape: {df.shape}\nColumns: {list(df.columns)}\nHead:\n{df.head()}")

    return df


#########################################################
#                Merge DataFrames                       #
#########################################################

@task(
    name="Merge DataFrames",
    description="Merge totals, per_game, and advanced DataFrames into one.",
    tags=["NBA", "Basketball-Reference", "Stats", "Transformation"],
)
def merge_dfs(df1: pd.DataFrame, df2: pd.DataFrame, df3: pd.DataFrame) -> pd.DataFrame:
    """
    Merge player statistics DataFrames into one.

    Args:
        df1 (pd.DataFrame): The first DataFrame.
        df2 (pd.DataFrame): The second DataFrame.
        df3 (pd.DataFrame): The third DataFrame.

    Returns:
        pd.DataFrame: A merged DataFrame.
    """

    # Check player uniqueness and DataFrame shapes
    players1 = set(df1['Player'])
    players2 = set(df2['Player'])
    players3 = set(df3['Player'])


    if players1 == players2 == players3:
        print("All players are the same in each DataFrame.")
    else:
        print("[WARNING]: Players are not the same in each DataFrame.")

    print(f"\nDataFrame shape:\ndf1: {df1.shape}\ndf2: {df2.shape}\ndf3: {df3.shape}\n")
    print(f"DataFrame Unique Players:\ndf1: {len(players1)}\ndf2: {len(players2)}\ndf3: {len(players3)}\n")

    # Check for duplicates in DataFrames
    for i, df in enumerate([df1, df2, df3], start=1):
        if df.duplicated(subset=["Player", "Tm"]).any():
            print(f"df{i} has duplicates.")

    # Merge DataFrames
    merged_df = pd.merge(df1, df2, how="inner", on=["Player", "Tm"])
    df = pd.merge(merged_df, df3, how="inner", on=["Player", "Tm"])

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
#              Test Players Uniqueness                  #
#########################################################

@task(
    name="Test Players Uniqueness",
    description="Test keystring in each dataframe to ensure uniqueness.",
    tags=["NBA", "Basketball-Reference", "Stats", "Test", "Data Quality"],
)
def test_players_uniqueness(stats_t: list[str], stats_a: list[str]) -> None:
    """
    Test the uniqueness of players in two lists.

    Args:
        stats_t (list[str]): The first list of player statistics.
        stats_a (list[str]): The second list of player statistics.

    Returns:
        None
    """
    # Convert lists to sets for comparison
    set_t = set(stats_t)
    set_a = set(stats_a)

    # Check for uniqueness
    are_equal = set_t == set_a
    t_unique = set_t.difference(set_a)
    a_unique = set_a.difference(set_t)

    # Logging information
    print(f"Are the players unique? {are_equal}")
    print(f"Players unique to totals: {t_unique}")
    print(f"Players unique to advanced: {a_unique}")


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

from BRScraper import nba
from prefect import task
import pandas as pd
import awswrangler as wr


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
    df = nba.get_stats(season=season, info=info)

    # Process and rename columns based on selected info
    if info in ("totals", "per_game"):
        print(f"Processing {info} stats...")
        df = df.drop(columns=['Pos', 'Age', 'G', 'Season'])
        if info == "per_game":
            print("Processing per_game stats...")
            df = df.drop(columns=['GS'])
            df = df.rename(columns={i: f"{i}_per_game" for i in df.columns if i not in ["Player", "Tm"]})
        elif info == "totals":
            print("Processing totals stats...")
            df = df.drop(columns=['MP'])
            df = df.rename(columns={i: f"{i}_totals" for i in df.columns if i not in ["Player", "Tm"]})
    elif info == "advanced":
        print("Processing advanced stats...")
        df = df.rename(columns={i: f"{i}_advanced" for i in df.columns if i not in ["Player", "Tm", "Pos", "Age", "G", "MP", "Season"]})

    # Logging information
    print(f"Season: {season}\nInfo: {info}\nShape: {df.shape}\nColumns: {list(df.columns)}\nHead:\n{df.head()}")

    return df

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
    df = pd.merge(df1, df2, how="inner", on=["Player", "Tm"])
    df = pd.merge(df, df3, how="inner", on=["Player", "Tm"])

    # Logging information
    print(f"Shape: {df.shape}\nColumns: {list(df.columns)}\nHead:\n{df.head()}")

    return df

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
    df['snapshot_date'] = snapshot_date

    # Logging information
    print(df.head())

    return df


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
    set_t = set(stats_t)
    set_a = set(stats_a)

    # Logging information
    print(set_t == set_a)
    print(set_t.difference(set_a))
    print(set_a.difference(set_t))


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
    wr.s3.to_parquet(
        df=df,
        path=f"s3://{bucket_name}/data/raw/players/{current_day}.parquet"
    )

    # Logging information
    print("Data saved to S3 bucket.")

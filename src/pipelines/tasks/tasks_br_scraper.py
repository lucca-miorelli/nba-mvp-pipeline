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
def get_stats(season: str = "2023", info: str = "totals") -> pd.DataFrame:
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

    # Define a dictionary to map "info" to columns to drop
    columns_to_drop_mapping = {
        "per_game": ["G", "GS", "Pos", "Age"],
        "totals": ["MP", "G", "Pos", "Age"]
    }

    # Drop unnecessary columns based on selected info
    columns_to_drop = columns_to_drop_mapping.get(info, []) + ['Season']
    df = df.drop(columns=columns_to_drop)

    # Rename columns based on selected info
    if info == "per_game":
        columns_to_exclude = ["Player", "Tm"]
        new_column_suffix = "_per_game"
    elif info == "totals":
        columns_to_exclude = ["Player", "Tm", "Pos", "Age"]
        new_column_suffix = "_totals"
    else:  # For "advanced" info
        columns_to_exclude = ["Player", "Tm", "Pos", "Age", "Season"]
        new_column_suffix = "_advanced"

    # Rename columns
    df = df.rename(columns={i: f"{i}{new_column_suffix}" for i in df.columns if i not in columns_to_exclude})

    # Remove * from Player column
    df['Player'] = df['Player'].str.replace("*", "")
    
    # Log information
    print(f"Processing {info} stats...")
    print(f"Season: {season}\nInfo: {info}\nShape: {df.shape}\nColumns: {list(df.columns)}\nHead:\n{df.head()}")

    return df


#########################################################
#                  Get Team Standings                   #
#########################################################

@task(
    name="Get Standings",
    description="Get team standings from basketball-reference.com",
    tags=["NBA", "Basketball-Reference", "Stats", "Extraction"],
)
def get_standings(season: str = "2023", info: str = "total") -> pd.DataFrame:
    df = nba.get_standings(season=season, info=info)

    # Remove * from Team column
    df['Tm'] = df['Tm'].str.replace("*", "")

    dict_teams = {'Utah Jazz':'UTA','Phoenix Suns':'PHO',
                'Philadelphia 76ers':'PHI','Brooklyn Nets':'BRK',
                'Denver Nuggets':'DEN','Los Angeles Clippers':'LAC',
                'Milwaukee Bucks':'MIL','Dallas Mavericks':'DAL',
                'Los Angeles Lakers':'LAL','Portland Trail Blazers':'POR',
                'Atlanta Hawks':'ATL','New York Knicks':'NYK',
                'Miami Heat':'MIA','Golden State Warriors':'GSW',
                'Memphis Grizzlies':'MEM','Boston Celtics':'BOS',
                'Washington Wizards':'WAS','Indiana Pacers':'IND',
                'Charlotte Hornets':'CHO','Charlotte Bobcats':'CHA',
                'San Antonio Spurs':'SAS','Chicago Bulls':'CHI',
                'New Orleans Pelicans':'NOP','Sacramento Kings':'SAC',
                'Toronto Raptors':'TOR','Minnesota Timberwolves':'MIN',
                'Cleveland Cavaliers':'CLE','Oklahoma City Thunder':'OKC',
                'Orlando Magic':'ORL','Detroit Pistons':'DET',
                'Houston Rockets':'HOU','New Jersey Nets':'NJN',
                'New Orleans Hornets':'NOH','Seattle SuperSonics':'SEA'}

    # Replace team names with abbreviations
    df['Tm'] = df['Tm'].replace(dict_teams)

    # Add sufix _team to columns except for Tm and Seed
    columns_to_exclude = ["Tm", "Seed"]
    df = df.rename(columns={i: f"{i}_team" for i in df.columns if i not in columns_to_exclude})

    # Remove rows with Tm in ['Southwest Division', 'Atlantic Division', 'Central Division', 'Southeast Division', 'Northwest Division', 'Pacific Division']
    df = df[~df['Tm'].isin(['Southwest Division', 'Atlantic Division', 'Central Division', 'Southeast Division', 'Northwest Division', 'Pacific Division'])]

    # Reset index
    df = df.reset_index(drop=True)

    # Log information
    print(f"Processing {info} standings...")
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
    
    return dataframes

#########################################################
#                Merge DataFrames                       #
#########################################################

@task(
    name="Merge Stat DataFrames",
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
#         Merge Standings and Stats DataFrames          #
#########################################################

@task(
    name="Merge Standings and Stats DataFrames",
    description="Merge standings and stats DataFrames into one.",
    tags=["NBA", "Basketball-Reference", "Stats", "Transformation"],
)
def merge_standings_and_stats(standings_df: pd.DataFrame, stats_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge standings and stats DataFrames into one.

    Args:
        standings_df (pd.DataFrame): The standings DataFrame.
        stats_df (pd.DataFrame): The stats DataFrame.

    Returns:
        pd.DataFrame: A merged DataFrame.
    """
    # Log information
    print(f"Standings:\nShape: {standings_df.shape}\nColumns: {list(standings_df.columns)}\nHead:\n{standings_df.head()}")
    print(f"Stats:\nShape: {stats_df.shape}\nColumns: {list(stats_df.columns)}\nHead:\n{stats_df.head()}")

    # Merge DataFrames on Tm
    df = pd.merge(stats_df, standings_df, on=["Tm"], how="left")

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
#             Add Season Column                         #
#########################################################

@task(
    name="Add Season Column",
    description="Add a season column to the DataFrame.",
    tags=["NBA", "Basketball-Reference", "Stats", "Transformation"]
)
def add_season_column(df: pd.DataFrame, season: str) -> pd.DataFrame:
    """
    Add a season column to the DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame.
        season (str): The NBA season ("2023")

    Returns:
        pd.DataFrame: The DataFrame with the added season column.
    """
    # Format season
    season = f"{str(int(season)-1)}-{season[2:]}"

    # Add snapshot date column
    df['season'] = season

    # Logging information
    print(f"Added season column with value {season}.")

    return df


#########################################################
# Define data types for each column in the DataFrame    #
#########################################################

@task
def define_column_data_types(dataframe, column_data_types):
    """
    Defines the data type of each specified column in a DataFrame.
    
    Args:
        dataframe (pd.DataFrame): The DataFrame to be modified.
        column_data_types (dict): A dictionary mapping column names to their intended data types.
        
    Returns:
        pd.DataFrame: The modified DataFrame with the defined column data types.
    """
    for column, data_type in column_data_types.items():
        if column in dataframe.columns:
            dataframe[column].fillna(0, inplace=True)                 # This is temporary until I figure out how to deal with NaNs
            dataframe[column] = dataframe[column].astype(data_type)
            print(f"Column '{column}' data type changed to '{data_type}'.")
        else:
            print(f"Column '{column}' not found in the DataFrame.")
    return dataframe

    

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

#########################################################
#          Ingest Historical Data into S3 Bucket        #
#########################################################

@task(
    name="Ingest Historical Data",
    description="Save historical data to S3 bucket as parquet.",
    tags=["NBA", "Basketball-Reference", "Stats", "Ingestion"],
)
def load_historical_data(df: pd.DataFrame, bucket_name: str, season: str) -> None:

    # Construct the S3 path for saving parquet file
    s3_path = f"s3://{bucket_name}/data/raw/historical/{season}.parquet"

    wr.s3.to_parquet(df=df, path=s3_path)

    # Logging information
    print("Historical data saved to S3 bucket.")

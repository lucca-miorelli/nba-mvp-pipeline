#########################################################
#                IMPORT LIBRARIES                       #
#########################################################

import pandas as pd
import awswrangler as wr
import time
from prefect import task, flow
from prefect.runtime import flow_run
from datetime import datetime
from typing import List


#########################################################
#                 GLOBAL VARIABLES                      #
#########################################################

BUCKET_PATH = "s3://nba-mvp-pipeline/{}"
SEASONS = [str(i) for i in range(2007, 2024)]  # Seasons from 2006-07 to 2022-23


#########################################################
#                  HELPER FUNCTIONS                     #
#########################################################

# Funciton that generates flow_run_name based on data
def generate_flow_run_name():
    """
    Generates a flow run name based on the current date and time.
    
    Returns:
        str: The generated flow run name.
    """
    today = datetime.today()
    return today.strftime("%Y-%m-%d-%Hh-%Mmin-%Ss")


#########################################################
#                  TASKS DEFINITION                     #
#########################################################

@task(
    name="Read MVP Data",
    description="Read MVP data from S3",
    tags=["NBA", "S3", "MVP", "Read"]
)
def read_mvp_data():
    """
    Reads MVP data from an S3 bucket and returns a DataFrame.
    
    Args:
        None
        
    Returns:
        pd.DataFrame: The DataFrame containing the data from the Parquet file.
    """
    # Read MVP data from S3
    df_mvp = wr.s3.read_parquet(BUCKET_PATH.format(f"data/raw/mvp/mvp.parquet"))

    print(df_mvp.head())
    print(df_mvp.shape)

    return df_mvp

@task(
    name="Read Stats Data",
    description="Read stats data from S3",
    tags=["NBA", "S3", "Stats", "Read"]
)
def read_stats_from_s3(path):
    """
    Reads a Parquet file from an S3 bucket and returns a DataFrame.
    
    Args:
        path (str): The S3 path to the Parquet file.
        
    Returns:
        pd.DataFrame: The DataFrame containing the data from the Parquet file.
    """
    df_stats = wr.s3.read_parquet(path)
    print("This is the shape of the DataFrame: ", df_stats.shape)

    return df_stats


@task(
    name="Filter Players",
    description="Filters players that played for more than one team in a given season",
    tags=["NBA", "Stats", "Transform"]
)
def filter_players(df_stats):
    """
    Filters players that played for more than one team in a given season.
    If a player played for more than one team, the team name is 'TOT'.
    
    Args:
        df_stats (pd.DataFrame): The DataFrame to filter.
        
    Returns:
        pd.DataFrame: The filtered DataFrame.
    """
    # Selects only players with one team or TOT
    df_stats_filtered = df_stats.groupby("Player").agg({
        "Tm": lambda x: 'TOT' if len(x.unique()) > 1 else x.unique()[0]
    }).reset_index()

    # Merges the filtered DataFrame with the original DataFrame to get the other columns
    df_stats_filtered = pd.merge(
        df_stats_filtered,
        df_stats,
        on=["Player", "Tm"],
        how="left"
    )

    print("This is the shape of the filtered DataFrame: ", df_stats_filtered.shape, "\n")

    return df_stats_filtered


@task(
    name="Merge DataFrames",
    description="Merges the MVP data with historical stats data using a left join on 'Player' and 'Season'",
    tags=["NBA", "Stats", "MVP", "Transform"]
)
def merge_stats_with_mvp(df_stats, df_mvp):
    """
    Merges the MVP data with the historical stats data using a left join on 'Player' and 'Season'.
    
    Args:
        df_mvp (pd.DataFrame): DataFrame containing MVP data.
        df_stats (pd.DataFrame): DataFrame containing historical stats data.
        
    Returns:
        pd.DataFrame: Merged DataFrame.
    """
    print("This is the shape of the MVP DataFrame: ", df_mvp.shape)
    print("This is the shape of the stats DataFrame: ", df_stats.shape)

    # Merge DataFrames
    merged_df = pd.merge(
        df_stats,
        df_mvp,
        left_on=["Player", "season"],
        right_on=["Player", "Season"],
        how="left"
    )

    print("This is the shape of the merged DataFrame: ", merged_df.shape)
    print("This is the shape of merged_df where Share is not null: ", merged_df[merged_df['Share'].notnull()].shape)


    return merged_df

@task(
    name="Find Missing Players",
    description="Finds the 'Player' and 'Season' from df_mvp that do not have a match in df_stats",
    tags=["NBA", "Stats", "MVP", "Data Quality"]
)
def find_missing_players(df_stats, df_mvp):
    """
    Finds the "Player" and "Season" from df_mvp that do not have a match in df_stats.
    Only runs if the merge was unsuccessful.

    Args:
        df_stats (pd.DataFrame): The DataFrame containing the merged data.
        df_mvp (pd.DataFrame): The DataFrame containing the MVP data.

    Returns:
        pd.DataFrame: The DataFrame containing the missing "Player" and "Season" values.
    """
    # Merge DataFrames
    merged_df = pd.merge(
        df_mvp,
        df_stats,
        left_on=["Player", "Season"],
        right_on=["Player", "season"],
        how="outer",
        indicator=True
    )

    # Get missing values
    missing_df = merged_df[merged_df['_merge'] == 'left_only'][['Player', 'Season']]

    print("These are the missing 'Player' and 'Season' values:")
    print(missing_df)

    return missing_df

@task(
    name="Handle Null Values",
    description="Handle null values in a merged statistics DataFrame",
    tags=["NBA", "Stats", "MVP", "Data Quality"]
)
def handle_null_values(df_stats):
    """
    Handle null values in a merged statistics DataFrame.

    This function performs the following operations on the input DataFrame:
    1. Fills null values in the 'Share' column with 0.
    2. Drops the 'Season' and 'MVP Rank' columns from the DataFrame.
    3. Checks for any remaining null values and prints their shape and rows.

    Args:
        df_stats (pandas.DataFrame): The merged statistics DataFrame to be processed.

    Returns:
        pandas.DataFrame: The processed DataFrame with null values handled and specified columns dropped.
    """

    # Fill null values with 0
    df_stats['Share'].fillna(0, inplace=True)
    
    # Drops columns
    df_stats.drop(columns=['Season', 'Rank'], inplace=True)

    # Check for null values
    print("Shape of the processed DataFrame: ", df_stats.shape)
    print(df_stats.head())


    return df_stats


@task(
    name="Check Player Name Matches",
    description="Check if all MVP award recipients for a specified season have corresponding statistics data",
    tags=["NBA", "Stats", "MVP", "Data Quality"]
)
def check_player_name_matches(df_mvp, df_stats, seasons):
    """
    Check if names in MVP dataframe have matched in the stats dataframe.

    This function compares the player names between an MVP DataFrame and a statistics DataFrame
    to determine whether all players who received the MVP award for a specified season have
    corresponding statistics data in the player statistics DataFrame.

    Args:
        df_mvp (pandas.DataFrame): DataFrame containing MVP award data, including 'Season' and 'Player' columns.
        df_stats (pandas.DataFrame): DataFrame containing player statistics data, including a 'Player' and 'season' columns.
        season (List[str]): List of seasons to check (e.g., ['2020-21', '2021-22']

    Returns:
        bool: True if all seasons have matches, False otherwise.
    """
    bool_list = []

    for season in seasons:
        # Get MVP players for the given season
        mvp_players = df_mvp[df_mvp['Season'] == season]['Player'].unique()
        # Get players in the stats DataFrame
        stats_players = df_stats['Player'].unique()
        # Check if all MVP players for the given season have matches in the stats DataFrame
        bool_list.append(all(player in stats_players for player in mvp_players))

    return all(bool_list)


@task(
    name="Load Processed Data to S3",
    description="Load processed data to S3 bucket",
    tags=["NBA", "S3", "Stats", "Ingestion"]
)
def load_processed_data(df_stats_processed, path):
    """
    Saves processed data DataFrame to an S3 bucket in Parquet format.

    Args:
        df_stats_processed (pd.DataFrame): The DataFrame containing processed data.
        path (str): The S3 path to save the Parquet file.

    Returns:
        None
    """
    try:
        wr.s3.to_parquet(
            df=df_stats_processed,
            path=path,
        )
    except Exception as e:
        print(e)
    else:
        print("Data loaded successfully!")


#########################################################
#                  SUBFLOW DEFINITION                   #
#########################################################

# Create flow that reads data from s3 and filters it
@flow(
    name="Read and Filter Historical Data",
    description="Read and filter data for the NBA MVP pipeline",
    flow_run_name=generate_flow_run_name,
)
def read_and_filter_stats(seasons:List[str])->pd.DataFrame:
    """
    Reads data from S3 for all seasons and filters it.

    Args:
        seasons (List[str]): List of seasons to read and filter (e.g., ['2020-21', '2021-22']).

    Returns:
        pd.DataFrame: Concatenated DataFrame containing the filtered data.
    """
    all_dataframes = []

    for season in seasons:

        print(f"Reading data for {season} season...")

        stats_raw_path = BUCKET_PATH.format(f"data/raw/historical/{season[:4]}.parquet")
        
        # Read data from S3
        df_stats_season = read_stats_from_s3(stats_raw_path)

        # Filter players that played for more than one team
        df_stats_filtered = filter_players(df_stats_season)

        # Append to list of DataFrames
        all_dataframes.append(df_stats_filtered)
    
    # Concatenate DataFrames
    df_stats = pd.concat(all_dataframes, ignore_index=True)

    return df_stats



#########################################################
#                   FLOW DEFINITION                     #
#########################################################

@flow(
    name="Process Historical Data",
    description="Process historical data for the NBA MVP pipeline",
    flow_run_name=generate_flow_run_name,
    log_prints=True
)
def process_data():
    """
    Gets historical data from S3, processes it, and saves it back to S3.

    This prefect.flow performs the following operations:
    1. Reads data from S3.
    2. Filters players that played for more than one team.
    3. Merges the MVP data with the current season data.
    4. Handles null values.
    5. Saves the processed data to S3.

    Args:
        None
    
    Returns:
        None
    """
    
    processed_data_path = BUCKET_PATH.format(f"data/processed/mvp/stats_mvp.parquet")

    # Call subflow for reading and filtering
    df_stats = read_and_filter_stats(SEASONS)

    # Read MVP data from S3
    df_mvp   = read_mvp_data()
    
    # Check if MVP player names for the given season have matches in the stats DataFrame
    result = check_player_name_matches(df_mvp, df_stats, SEASONS)

    if result:
        print("All MVP players for the given season have matches in the stats DataFrame.")
    else:
        print("Not all MVP players for the given season have matches in the stats DataFrame.")
        raise Exception("Not all MVP players for the given season have matches in the stats DataFrame.")
    
    # Merge DataFrames
    df_stats_merged = merge_stats_with_mvp(df_stats, df_mvp)

    if df_stats_merged[df_stats_merged['Share'].notnull()].shape[0] == df_mvp.shape[0]:
        print("Merge successful!")
    else:
        print("[ERROR] Merge unsuccessful!")

        # Find missing players
        find_missing_players(df_stats, df_mvp)

        # Raise exception
        raise Exception("Merge unsuccessful!")

    # Handle null values
    df_stats_processed = handle_null_values(df_stats_merged)

    # Save processed data to S3
    load_processed_data(df_stats_processed, processed_data_path)


#########################################################
#                       RUN FLOW                        #
#########################################################

if __name__ == "__main__":
    # Process data
    process_data()
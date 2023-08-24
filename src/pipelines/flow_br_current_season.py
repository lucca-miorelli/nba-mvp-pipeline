#########################################################
#                IMPORT LIBRARIES                       #
#########################################################

from prefect import flow
from tasks.tasks_br_scraper import (
    get_stats,
    merge_dfs,
    add_date_column,
    load_data,
    check_players_and_duplicates,
    add_season_column,
    define_column_data_types
)
from datetime import datetime
from tasks.data_types import data_types



#########################################################
#                 GLOBAL VARIABLES                      #
#########################################################

CURRENT_SEASON = "2023" # "2022-23"
CURRENT_DAY    = datetime.now()
BUCKET_NAME    = "nba-mvp-pipeline"

#########################################################
#                 HELPER FUNCTIONS                      #
#########################################################

def flow_run_name_generator():
    """
    Generates a flow run name for the Prefect flow.
    
    Returns:
        str: The flow run name in the format "YYYY_MM_DD"
    """
    return CURRENT_DAY.strftime("%Y_%m_%d")


#########################################################
#                   FLOW DEFINITION                     #
#########################################################

@flow(name="[BRef] Current Season Data Scraper", flow_run_name=flow_run_name_generator, log_prints=True)
def scrap_current_season_stats() -> None:
    """
    Scrapes current NBA player statistics from Basketball Reference.
    Makes three requests to the website, one for each type of statistics (advanced, totals, per game).
    Apply simple data cleaning and transformation.
    Loads the data into an S3 `nba-mvp-pipeline/data/raw/{date}.parquet`.
    
    Args:
        date (str): The date of the snapshot in the format "YYYY_MM_DD".
        bucket_name (str): The name of the S3 bucket.
        
    Returns:
        None
    """
    # Get player statistics for different types
    df_totals   = get_stats(season=CURRENT_SEASON, info="advanced")
    df_advanced = get_stats(season=CURRENT_SEASON, info="totals")
    df_pergame  = get_stats(season=CURRENT_SEASON, info="per_game")

    # Check for players and duplicates
    check_players_and_duplicates([df_totals, df_advanced, df_pergame])

    # Merge DataFrames
    merged_df = merge_dfs([df_totals, df_advanced, df_pergame])

    # Add snapshot date column
    df_with_date  = add_date_column(merged_df, CURRENT_DAY)

    # Add season column
    df_with_season = add_season_column(df_with_date, CURRENT_SEASON)

    # Column data types
    df_transformed = define_column_data_types(df_with_season, data_types)

    # Load data into S3 bucket
    load_data(df_transformed, BUCKET_NAME, CURRENT_DAY.strftime("%Y_%m_%d"))


#########################################################
#                       MAIN                            #
#########################################################

if __name__ == "__main__":
    scrap_current_season_stats()

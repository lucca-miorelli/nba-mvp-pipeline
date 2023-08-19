#########################################################
#                   IMPORT LIBRARIES                    #
#########################################################

from prefect import flow
from tasks.tasks_br_scraper import get_stats
from tasks.tasks_br_scraper import merge_dfs, check_players_and_duplicates, load_historical_data, add_season_column, define_column_data_types
from tasks.data_types import data_types


#########################################################
#                 GLOBAL VARIABLES                      #
#########################################################

SEASONS = [str(i) for i in range(2007, 2024)] # 2006-07 to 2022-23
BUCKET_NAME = "nba-mvp-pipeline"


#########################################################
#                  SUB-FLOW DEFINITION                  #
#########################################################

@flow(name="Extract Historical Data", flow_run_name="{season}", log_prints=True, validate_parameters=False)
def extract_stats(season: str):
    """
    Extracts player statistics for a given season from Basketball Reference.
    
    Args:
        season (str): The NBA season in the format "YYYY" (e.g., "2023" == "2022-23").
        
    Returns:
        dict: A dictionary containing three DataFrames, one for each type of statistics (advanced, totals, per game).
    """
    # Get player statistics for different types
    df_advanced = get_stats(season=season, info="advanced")
    df_totals = get_stats(season=season, info="totals")
    df_pergame = get_stats(season=season, info="per_game")

    return [df_totals, df_advanced, df_pergame]


#########################################################
#                   FLOW DEFINITION                     #
#########################################################

@flow(name="[BRef] Historical Data Scraper", flow_run_name="Season {season}", log_prints=True)
def historical_data_scraper(season: str):
    """
    Scrapes historical NBA player statistics from Basketball Reference for the given season.
    Makes three requests to the website, one for each type of statistics (advanced, totals, per game).
    Apply simple data cleaning and transformation.
    Loads the data into an S3 `nba-mvp-pipeline/data/raw/historical/{season}.parquet`.
    
    Args:
        season (str): The NBA season in the format "YYYY" (e.g., "2023" == "2022-23").
        
    Returns:
        None
    """
    #########################################################
    #                        EXTRACT                        #
    #########################################################

    # Extract player statistics for different types
    stats_list = extract_stats(season=season)

    #########################################################
    #                      TRANSFORM                        #
    #########################################################

    # Check for players and duplicates
    check_players_and_duplicates(stats_list)

    # Merge DataFrames
    merged_df = merge_dfs(stats_list)

    # Add season column
    df_with_season = add_season_column(merged_df, season)

    # Define column data types
    df_transformed = define_column_data_types(df_with_season, data_types)

    #########################################################
    #                         LOAD                          #
    #########################################################

    # Load data to S3 bucket
    load_historical_data(df_transformed, BUCKET_NAME, season)



if __name__ == "__main__":

    # Run flow for each season
    for season in SEASONS:
        historical_data_scraper(season=season)

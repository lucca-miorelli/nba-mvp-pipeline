#########################################################
#                IMPORT LIBRARIES                       #
#########################################################

from prefect import flow
from tasks.tasks_br_scraper import get_stats, merge_dfs, add_date_column, load_data, check_players_and_duplicates
from datetime import datetime


#########################################################
#                 GLOBAL VARIABLES                      #
#########################################################

CURRENT_SEASON = "2023" # "2022-23"
CURRENT_DAY    = datetime.now()
BUCKET_NAME    = "nba-mvp-pipeline"


#########################################################
#                   FLOW DEFINITION                     #
#########################################################

@flow
def scrap_current_season_stats():
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

    # Load data into S3 bucket
    load_data(df_with_date, BUCKET_NAME, CURRENT_DAY.strftime("%Y_%m_%d"))


#########################################################
#                       MAIN                            #
#########################################################

if __name__ == "__main__":
    scrap_current_season_stats()

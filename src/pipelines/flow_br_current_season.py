from prefect import flow
from tasks.tasks_br_scraper import get_stats, merge_dfs, add_date_column, load_data
from datetime import datetime


CURRENT_SEASON = "2023" # "2022-23"
CURRENT_DAY    = datetime.now().strftime('%Y_%m_%d')
BUCKET_NAME    = "nba-mvp-pipeline"


@flow
def scrap_current_season_stats():
    
    df_totals   = get_stats(season=CURRENT_SEASON, info="advanced")
    df_advanced = get_stats(season=CURRENT_SEASON, info="totals")
    df_pergame  = get_stats(season=CURRENT_SEASON, info="per_game")
    
    # test_players_uniqueness(list_t, list_a)

    # rename columns (transformation)

    merged_df = merge_dfs(df_totals, df_advanced, df_pergame)

    df = add_date_column(merged_df, CURRENT_DAY)

    # load_data(df, BUCKET_NAME, CURRENT_DAY)


if __name__ == "__main__":
    scrap_current_season_stats()

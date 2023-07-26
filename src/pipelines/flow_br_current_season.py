from prefect import Flow
from tasks.tasks_br_scraper import get_stats, merge_three_dfs


CURRENT_SEASON = "2023"


@Flow
def scrap_current_season_stats():
    
    df_totals   = get_stats(season=CURRENT_SEASON, info="advanced")
    df_advanced = get_stats(season=CURRENT_SEASON, info="totals")
    df_pergame  = get_stats(season=CURRENT_SEASON, info="per_game")

    merged_df = merge_three_dfs(df_totals, df_advanced, df_pergame)

    # test_players_uniqueness(list_t, list_a)


if __name__ == "__main__":
    scrap_current_season_stats()

from BRScraper import nba
from prefect import task
import pandas as pd
import awswrangler as wr


# @Task
# def get_advanced_stats(season:str="2023", info:str=None):
    
#     df_stats_totals = nba.get_stats(season="2023", info="totals")
#     print(df_stats_totals.head())

#     df_stats_advanced = nba.get_stats(season="2023", info="advanced")
#     print(df_stats_advanced.head())

#     return list(df_stats_totals.Player), list(df_stats_advanced.Player)


@Task
def get_stats(season:str=None, info:str=None):
    
    df = nba.get_stats(season=season, info=info)

    print(f"Season: {season}\nInfo: {info}\n")
    print(df.head())

    return df

@Task
def merge_three_dfs(df1, df2, df3):

    df = df1.merge(df2, on='Player')
    df = df.merge(df3, on='Player')

    print(df.head())

    return df

@Task
def test_players_uniqueness(stats_t:list[str], stats_a:list[str]):

    set_t = set(stats_a)
    set_a = set(stats_t)

    print(set_t == set_a)
    print(set_t.difference(set_a)) 
    print(set_a.difference(set_t))

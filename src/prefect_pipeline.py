from prefect import flow, task
from utils.nba_requests import NbaAPI
from nba_api.stats.endpoints.leaguedashplayerstats import LeagueDashPlayerStats
import pandas as pd
import json


@task(
    name="Extract Data"
    ,description="Sends GET request to NBA endpoint, returns response object."
    ,tags=['extract']
)
def extract_league_leaders(PerMode:str):

    stats = NbaAPI()

    response = stats.get_leaders(
        LeagueID='00',
        PerMode=PerMode,
        Scope='S',
        Season='2022-23',
        SeasonType='Regular%20Season',
        StatCategory='PTS'
    )

    print(response.status_code)

    return response.json()



@task
def create_per_game_df(response:str=None):

    df = pd.DataFrame(
        data=response['resultSet']['rowSet'],
        columns=[
            value + '_PERGAME'
            if value not in ['PLAYER_ID','RANK','PLAYER','TEAM_ID','TEAM','GP']
            else value
            for value in response['resultSet']['headers']
        ]
    )

    # Drop columns that are not needed
    df = df.drop(columns=['RANK', 'PLAYER', 'TEAM', 'TEAM_ID', 'GP'])

    return df


@task
def create_totals_df(response:str=None):

    df = pd.DataFrame(
        data=response['resultSet']['rowSet'],
        columns=[
            value + '_TOTAL' 
            if value not in ['PLAYER_ID','RANK','PLAYER','TEAM_ID','TEAM','GP']
            else value
            for value in response['resultSet']['headers']
        ]
    )

    return df


@task
def join_dfs(df_per_game, df_totals):

    df = df_per_game.merge(df_totals, on='PLAYER_ID')

    return df

@task
def extract_advanced_stats():

    stats = LeagueDashPlayerStats(
        per_mode_detailed="PerGame",
        league_id_nullable='00',
        measure_type_detailed_defense='Advanced'
    )

    stats.get_request()

    print(stats.nba_response._status_code)
    print(stats.get_request_url())

    df = stats.data_sets[0].get_data_frame()

    # Drop columns that are not needed
    df = df.drop(columns=['PLAYER_NAME', 'TEAM_ID', 'NICKNAME', 'TEAM_ABBREVIATION', 'GP', 'AGE', 'MIN'])

    return df


@flow(
    name="NBA Players Statistics ETL"
    ,description=\
        "Extracts players's statistics from stats.nba.com/leagueLeaders."
    ,version="v01"
)
def pipeline_league_leaders():

    # Extract LeagueLeaders
    response_per_game   = extract_league_leaders(PerMode='PerGame')
    response_totals     = extract_league_leaders(PerMode='Totals')
    
    # Extract Advanced Stats
    df_advanced         = extract_advanced_stats()

    # Return LeagueLeaders dataframes
    df_per_game         = create_per_game_df(response_per_game)
    df_totals           = create_totals_df(response_totals)

    # Join dataframes
    df_leaders = join_dfs(df_per_game, df_totals)

    # join advanced stats
    df = df_leaders.merge(df_advanced, on='PLAYER_ID')



    return json.dumps(dict(columns=df.columns.values.tolist()), indent=4)



print(pipeline_league_leaders())

# flow.register(project_name="NBA")
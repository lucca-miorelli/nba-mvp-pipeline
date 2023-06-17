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
def join_advanced_stats(df_leaders, df_advanced):

    df = df_leaders.merge(df_advanced, on='PLAYER_ID')

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
@flow(
    name="[EXTRACT] Advanced Stats"
    ,description="Extract data from NBA.com/AdvancedStats endpoint."
)
def pipeline_advanced_stats()->pd.DataFrame:
    
        # Call extract_advanced_stats task
        response_advanced = extract_advanced_stats()
    
        return response_advanced

@flow(
     name="[TRANSFORM] LeagueLeaders"
    ,description="Transform data from NBA.com/LeagueLeaders endpoint."
)
def pipeline_transformation(
     response_per_game
     ,response_totals
     ,response_advanced
    )->pd.DataFrame:
    
        # Call create_per_game_df task
        df_per_game = create_per_game_df(response_per_game)
        print(df_per_game.head())
    
        # Call create_totals_df task
        df_totals = create_totals_df(response_totals)
        print(df_totals.head())

        # Call join_dfs task
        df_leaders = join_dfs(df_per_game, df_totals)

        # Join LeagueLeaders and Advanced Stats
        df = join_advanced_stats(df_leaders, response_advanced)

    
        return df_leaders




@flow(
    name="NBA Stats ETL"
    ,description="Full ETL pipeline of NBA data."
    ,version="v01"
)
def nba_etl()->str:

    # Extract LeagueLeaders data
    response_per_game, response_totals = pipeline_league_leaders()

    # Extract Advanced Stats
    response_advanced = pipeline_advanced_stats()

    # Transform LeagueLeaders data
    df = pipeline_transformation(response_per_game, response_totals, response_advanced)

    

    return json.dumps(dict(columns=df.columns.values.tolist()), indent=4)

print(pipeline_league_leaders())

# flow.register(project_name="NBA")
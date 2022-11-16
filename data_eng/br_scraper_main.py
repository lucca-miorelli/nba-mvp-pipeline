#from basketball_reference_scraper.teams import get_roster_stats, get_roster
import pandas as pd
import numpy as np
import unidecode
from datetime import datetime
import time
import os

from br_scraper_func import get_team_misc, get_roster_stats, get_roster, fix_names


###############################################################################
#                                VARIABLES                                    #
###############################################################################

PATH_SAVE = os.path.join('data', '{}.parquet')

SEASON = ['2023']

FRANCHISES = [
    'UTA','PHO','PHI','BRK','DEN','LAC','MIL','DAL',
    'LAL','POR','ATL','NYK','MIA','GSW','MEM','BOS',
    'WAS','IND','CHO','SAS','CHI','NOP','SAC','TOR',
    'MIN','CLE','OKC','ORL','DET','HOU'
]


###############################################################################
#                                   SCRIPT                                    #
###############################################################################

per_game_total = pd.DataFrame()
advanced = pd.DataFrame()
infos = pd.DataFrame()
teams = pd.DataFrame()

time_sleep = 6.5

# Pegando dados para 2022-23
for season in SEASON:
    # Pegando dados para cada time
    for franchise in FRANCHISES:
        try:
            # Informações do time
            team = get_team_misc(franchise, season)
            print(team)
            team = pd.DataFrame(team).T
            team['G_TEAM'] = team['W'] + team['L']
            team = team[['G_TEAM','W','TEAM','SEASON']]
            team['PCT'] = team['W']/team['G_TEAM']
            teams = pd.concat([teams,team], ignore_index=True)
            time.sleep(time_sleep)

            # Informações gerais jogador
            info = get_roster(franchise, season)
            print(info)
            info['SEASON'] = team['SEASON'].iloc[0]
            infos = pd.concat([infos,info], ignore_index=True)
            time.sleep(time_sleep)
            
            # Stats avançadas
            avancado = get_roster_stats(franchise, season, data_format='ADVANCED', playoffs=False)
            print("This is avancado")
            print(avancado)
            for x in avancado.columns:
                if x not in ['G','PLAYER','POS','AGE','TEAM','SEASON','MP',
                            'Unnamed: 24','Unnamed: 19']:
                    avancado = avancado.rename(columns={x:x+'_ADVANCED'})
            advanced = pd.concat([advanced,avancado], ignore_index=True)
            time.sleep(time_sleep)

            # Stats por jogo
            pergame = get_roster_stats(franchise, season, data_format='PER_GAME', playoffs=False)
            print(pergame)

            # Criando variáveis totais
            pergame[['G','GS']] = (pergame[['G','GS']]).astype(int)
            cols = pergame.columns
            for x in cols:
                if x not in ['G','GS','PLAYER','POS','AGE','TEAM','SEASON',
                            'FG%','FT%','2P%','3P%','eFG%']:
                    try:
                        pergame[x] = (pergame[x]).astype(float)
                        pergame[x+'_TOTAL'] = round(pergame[x]*pergame['G'],0)
                        pergame = pergame.rename(columns={x:x+'_PERGAME'})
                    except Exception as e:
                        print(e)

            per_game_total = pd.concat([per_game_total,pergame], ignore_index=True)
            time.sleep(time_sleep)
        except Exception as e:
            # Time não existia na temporada
            print(e)
            raise Exception
            
    print(season)


### TRATAMENTO VARIÁVEIS ###

# Marcação College
infos['COLLEGE'] = np.where(infos['COLLEGE'].notna(),1,0)
# Marcação americano
infos['NATIONALITY_US'] = np.where(infos['NATIONALITY']=='US',1,0)
# Retirando colunas desnecessárias
infos = infos.drop(['NUMBER','POS','BIRTH_DATE',
                    'NATIONALITY'],axis=1)
# Retirando jogadores trocados
infos = infos.drop_duplicates(['PLAYER','SEASON'], keep=False, ignore_index=True)
infos = infos.fillna(0)
# Rookie = experience 0
infos['EXPERIENCE'] = (infos['EXPERIENCE'].replace('R',0)).astype(int)
# Altura em cm
infos['HEIGHT'] = ((infos['HEIGHT'].astype(str).str.split('-').str[0]).astype(int)*30.48)+((infos['HEIGHT'].astype(str).str.split('-').str[1]).astype(int)*2.54)
# Peso em kg
infos['WEIGHT'] = infos['WEIGHT']*0.453592
infos['IMC'] = infos['WEIGHT']/(infos['HEIGHT']/100)**2
              
# Retirando colunas repetidas/vazias
advanced = advanced.drop(['POS','AGE','TEAM','G','MP',
                            'Unnamed: 24','Unnamed: 19'], axis=1)
# Retirando jogadores trocados
advanced = advanced.drop_duplicates(['PLAYER','SEASON'], keep=False, ignore_index=True)
advanced = advanced.fillna(0)

per_game_total = per_game_total.drop_duplicates(['PLAYER','SEASON'], keep=False, ignore_index=True)
per_game_total[['FG%','FT%','2P%','3P%','eFG%']] = (per_game_total[['FG%','FT%','2P%','3P%','eFG%']]).astype(float)
per_game_total = per_game_total.fillna(0)

df = per_game_total.merge(advanced, on=['PLAYER','SEASON'], how='left', validate='1:1')
df = df.merge(infos, on=['PLAYER','SEASON'],how='left', validate='m:1')
df = df.merge(teams, on=['TEAM','SEASON'],how='left', validate='m:1')

# Criando SEED
# P/2022-23:
teams = teams[['TEAM','SEASON','PCT']][teams['SEASON']=='2022-23'].sort_values(by='PCT',ascending=False).reset_index(drop=True)
teams['SEED'] = teams.index + 1

df = df.merge(teams, on=['TEAM','SEASON','PCT'],how='left', validate='m:1')

df = df.fillna(-1)

numeric_columns = [
    "TS%_ADVANCED",
    "3PAr_ADVANCED",
    "FTr_ADVANCED",
    "ORB%_ADVANCED",
    "DRB%_ADVANCED",
    "TRB%_ADVANCED",
    "AST%_ADVANCED",
    "STL%_ADVANCED",
    "BLK%_ADVANCED",
    "TOV%_ADVANCED",
    "USG%_ADVANCED",
    "OWS_ADVANCED",
    "DWS_ADVANCED",
    "WS_ADVANCED",
    "WS/48_ADVANCED",
    "OBPM_ADVANCED",
    "DBPM_ADVANCED",
    "BPM_ADVANCED",
    "VORP_ADVANCED",
]


df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric)

print("This is final df")
print(df.head())
print(df.describe())

print("This is final df TS%_ADVANCED")
print(df["TS%_ADVANCED"])

print("printing dtypes...")
print(df.dtypes)

df.to_parquet(
    path=PATH_SAVE.format(
        str(datetime.today().strftime('%d_%m_%y'))
    ),
    index=False
)
import pandas as pd
from requests import get
from bs4 import BeautifulSoup, Comment
import unidecode


###############################################################################
#                                VARIABLES                                    #
###############################################################################

TEAM_SETS = [
    ['STL', 'TRI', 'MLH', 'ATL'],
    ['BOS'],
    ['NJN', 'BRK', 'NYN', 'NJA', 'NYA'],
    ['CHO', 'CHA', 'CHH'],
    ['CHI'],
    ['CLE'],
    ['DAL'],
    ['DEN', 'DNR', 'DNA'],
    ['DET', 'FTW'],
    ['GSW', 'SFW', 'PHW'],
    ['SDR', 'HOU'],
    ['INA', 'IND'],
    ['SDC', 'LAC', 'BUF'],
    ['LAL', 'MNL'],
    ['MEM', 'VAN'],
    ['MIA'],
    ['MIL'],
    ['MIN'],
    ['NOP', 'NOH', 'NOK'],
    ['NYK'],
    ['SEA', 'OKC'],
    ['ORL'],
    ['PHI', 'SYR'],
    ['PHO'],
    ['POR'],
    ['CIN', 'SAC', 'KCO', 'KCK', 'ROC'],
    ['DLC', 'SAA', 'SAS', 'TEX'],
    ['TOR'],
    ['NOJ', 'UTA'],
    ['WSB', 'CHP', 'CAP', 'BAL', 'WAS', 'CHZ']
]


###############################################################################
#                                FUNCTIONS                                    #
###############################################################################

def remove_accents(name, team, season_end_year):
    alphabet = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXZY ')
    if len(set(name).difference(alphabet))==0:
        return name
    r = get(f'https://www.basketball-reference.com/teams/{team}/{season_end_year}.html')
    team_df = None
    best_match = name
    if r.status_code==200:
        soup = BeautifulSoup(r.content, 'html.parser')
        table = soup.find('table')
        team_df = pd.read_html(str(table))[0]
        max_matches = 0
        for p in team_df['Player']:
            matches = sum(l1 == l2 for l1, l2 in zip(p, name))
            if matches>max_matches:
                max_matches = matches
                best_match = p
                
    return best_match

def get_roster(team, season_end_year):
    r = get(f'https://www.basketball-reference.com/teams/{team}/{season_end_year}.html')
    print(r)
    df = None
    if r.status_code == 200:
        soup = BeautifulSoup(r.content, 'html.parser')
        table = soup.find('table')
        df = pd.read_html(str(table))[0]
        df.columns = ['NUMBER', 'PLAYER', 'POS', 'HEIGHT', 'WEIGHT', 'BIRTH_DATE',
                      'NATIONALITY', 'EXPERIENCE', 'COLLEGE']
        # remove rows with no player name (this was the issue above)
        df = df[df['PLAYER'].notna()]
        df['PLAYER'] = df['PLAYER'].apply(
            lambda name: remove_accents(name, team, season_end_year))
        # handle rows with empty fields but with a player name.
        df['BIRTH_DATE'] = df['BIRTH_DATE'].apply(
            lambda x: pd.to_datetime(x) if pd.notna(x) else pd.NaT)
        df['NATIONALITY'] = df['NATIONALITY'].apply(
            lambda x: x.upper() if pd.notna(x) else '')

    return df

def get_roster_stats(team: list, season_end_year: int, data_format='PER_GAME', playoffs=False):
    if playoffs:
        period = 'playoffs'
    else:
        period = 'leagues'
    selector = data_format.lower()
    r = get(f'https://widgets.sports-reference.com/wg.fcgi?css=1&site=bbr&url=%2F{period}%2FNBA_{season_end_year}_{selector}.html&div=div_{selector}_stats',
            )
    df = None
    possible_teams = [team]
    for s in TEAM_SETS:
        if team in s:
            possible_teams = s
    if r.status_code == 200:
        soup = BeautifulSoup(r.content, 'html.parser')
        table = soup.find('table')
        df2 = pd.read_html(str(table))[0]
        for index, row in df2.iterrows():
            if row['Tm'] in possible_teams:
                if df is None:
                    df = pd.DataFrame(columns=list(row.index)+['SEASON'])
                row['SEASON'] = f'{str(int(season_end_year)-1)}-{str(season_end_year)[2:]}'
                df = pd.concat([df, row], axis='index')
        df.rename(columns={'Player': 'PLAYER', 'Age': 'AGE',
                          'Tm': 'TEAM', 'Pos': 'POS'}, inplace=True)
        df['PLAYER'] = df['PLAYER'].apply(
            lambda name: remove_accents(name, team, season_end_year))
        df = df.reset_index().drop(['Rk', 'index'], axis=1)
        
        return df

def get_team_misc(team, season_end_year):
    r = get(f'https://www.basketball-reference.com/teams/{team}/{season_end_year}.html',
            )
    df = None
    if r.status_code == 200:
        soup = BeautifulSoup(r.content, 'html.parser')
        comments = soup.find_all(string=lambda text: isinstance(text, Comment))
        tables = []
        for each in comments:
            if 'table' in str(each):
                try:
                    tables.append(pd.read_html(each, attrs = {'id': 'team_misc'}, header=1)[0])
                except:
                    continue
        df = pd.DataFrame(tables[0])
        df.columns = df.columns.str.replace(r'\.1','',regex=True)
        df.loc[:, 'SEASON'] = f'{str(int(season_end_year)-1)}-{str(season_end_year)[2:]}'
        df['TEAM'] = team
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        
        return pd.Series(index=list(df.columns), data=df.values.tolist()[0])


def fix_names(df):
    df['PLAYER'] = df['PLAYER'].str.upper()
    df['PLAYER'] = df['PLAYER'].str.replace('.','').str.replace("'",'').str.replace(' (TW)','')
    df['PLAYER'] = df['PLAYER'].apply(unidecode.unidecode)

    return df

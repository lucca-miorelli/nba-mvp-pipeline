import pandas as pd
import os

########## GETTING THE OLD DF COLS

# PATH = os.path.join('data', '01_12_22.parquet')

# df = pd.read_parquet(PATH)

# print(df.columns)



###### STARTING NEW SCRAPPER LEAGUE LEADERS
import requests
import urllib.parse
import json

BASE_URL = "https://stats.nba.com/stats/leagueLeaders"

payload=\
    dict(
        LeagueID='00',
        PerMode='Totals',
        Scope='S',
        Season='2022-23',
        SeasonType='Regular%20Season',
        StatCategory='PTS'
    )
payload_str = urllib.parse.urlencode(payload, safe='%')

result = requests.get(
    BASE_URL,
    params=payload_str
).json()

df = pd.DataFrame(
    data=result['resultSet']['rowSet'],
    columns=result['resultSet']['headers']
)
print(df)



# resource
# parameters
#     leagueid
#     permode
#     statcategory
#     season
#     seasontype
#     scope
#     activeflag
# resultSet
#     name
#     headers=[]
#     rowSet [[]]

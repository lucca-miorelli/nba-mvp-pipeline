from BRScraper import nba
from prefect import task
import pandas as pd
import awswrangler as wr


@task(
    name="Get Stats",
    description="Get stats from basketball-reference.com",
    tags=["NBA", "Basketball-Reference", "Stats", "Extraction"],
)
def get_stats(season:str=None, info:str=None)->pd.DataFrame:
    
    df = nba.get_stats(season=season, info=info)

    # Process and rename columns based on selected info
    if info in ("totals", "per_game"):
        print(f"Processing {info} stats...")
        df = df.drop(columns=['Pos', 'Age', 'G', 'Season'])
        if info == "per_game":
            print("Processing per_game stats...")
            df = df.drop(columns=['GS'])
            df = df.rename(columns={i: f"{i}_per_game" for i in df.columns if i not in ["Player", "Tm"]})
        elif info == "totals":
            print("Processing totals stats...")
            df = df.drop(columns=['MP'])
            df = df.rename(columns={i: f"{i}_totals" for i in df.columns if i not in ["Player", "Tm"]})
    elif info == "advanced":
        print("Processing advanced stats...")
        df = df.rename(columns={i: f"{i}_advanced" for i in df.columns if i not in ["Player", "Tm", "Pos", "Age", "G", "MP", "Season"]})

    # Logging information
    print(f"Season: {season}\nInfo: {info}\nShape: {df.shape}\nColumns: {list(df.columns)}\nHead:\n{df.head()}")

    return df

@task(
    name="Merge DataFrames",
    description="Merge totals, per_game, and advanced DataFrames into one.",
    tags=["NBA", "Basketball-Reference", "Stats", "Transformation"],
)
def merge_dfs(df1:pd.DataFrame, df2:pd.DataFrame, df3:pd.DataFrame)->pd.DataFrame:

    df1_player = df1["Player"].unique()
    df2_player = df2["Player"].unique()
    df3_player = df3["Player"].unique()

    """
    Check if the 'Player' column in each dataframe has the same values.
    """

    players1 = set(df1['Player'])
    players2 = set(df2['Player'])
    players3 = set(df3['Player'])

    if players1 == players2 == players3:
        print("True") 
    else:
        print("False")


    print(f"df1: {df1.shape}\ndf2: {df2.shape}\ndf3: {df3.shape}\n")
    print(f"df1: {len(df1_player)}\ndf2: {len(df2_player)}\ndf3: {len(df3_player)}\n")

    print(df1.duplicated(subset=["Player", "Tm"]).any())

    df = pd.merge(df1, df2, how="inner", on=["Player", "Tm"])
    df = pd.merge(df, df3, how="inner", on=["Player", "Tm"])

    print(df.shape)

    return df

@task(

)
def add_date_column(df:pd.DataFrame, snapshot_date:str)->pd.DataFrame:
    
    df['snapshot_date'] = snapshot_date

    print(df.head())

    return df


@task(
    name="Test Players Uniqueness",
    description="Test keystring in each dataframe to ensure uniqueness.",
    tags=["NBA", "Basketball-Reference", "Stats", "Test", "Data Quality"],
)
def test_players_uniqueness(stats_t:list[str], stats_a:list[str])->None:

    set_t = set(stats_a)
    set_a = set(stats_t)

    print(set_t == set_a)
    print(set_t.difference(set_a)) 
    print(set_a.difference(set_t))


@task(
    name="Ingest Data",
    description="Save data to S3 bucket as parquet.",
    tags=["NBA", "Basketball-Reference", "Stats", "Ingestion"],
)
def load_data(df:pd.DataFrame, bucket_name:str, current_day:str)->None:

    wr.s3.to_parquet(
        df=df
        ,path=f"s3://{bucket_name}/data/raw/players/{current_day}.parquet"
    )

    print("Data saved to S3 bucket.")

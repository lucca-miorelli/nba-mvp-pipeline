#########################################################
#                IMPORT LIBRARIES                       #
#########################################################

from prefect import flow, task
import pandas as pd
import awswrangler as wr
import time


#########################################################
#                 GLOBAL VARIABLES                      #
#########################################################

BUCKET_PATH = "s3://nba-mvp-pipeline/{}"
AWARD_URL = 'https://www.basketball-reference.com/awards/awards_{}.html'
SEASONS = [str(i) for i in range(2007, 2024)]  # Seasons from 2006-07 to 2022-23


#########################################################
#                  TASKS DEFINITION                     #
#########################################################

@task(
    name="Get MVP Data",
    description="Get MVP data from basketball-reference.com",
    tags=["NBA", "Basketball-Reference", "MVP", "Extraction"],
    task_run_name="{season}",
)
def get_mvp_data(award: str = 'mvp', season: str = None):
    """
    Retrieve MVP award data for a specific season.
    
    Args:
        award (str, optional): The type of award to retrieve (default is 'mvp').
        season (str): The season for which the data is requested in the format 'YYYY-YY'.
        
    Returns:
        pd.DataFrame: A DataFrame containing MVP award data for the specified season.
    """
    # Check if season is provided
    if season is None:
        raise ValueError('Season must be specified')
    else:
        print(f'Getting {award} data for {season} season...')

    # Construct the URL to fetch data
    url = AWARD_URL.format(season)

    # Read data from the URL and select the first table
    df = pd.read_html(url)[0]

    # Adjust columns to remove level 0 header
    df.columns = df.columns.droplevel(0)

    # Extract relevant columns
    df_mvp = df[['Rank', 'Player', 'Share']]

    # Extract the rank from 'Rank' column and update it
    df_mvp.loc[:, 'Rank'] = df_mvp['Rank'].astype(str).str.split('T', expand=True)[0]

    # Insert a 'Season' column based on the input season
    df_mvp.insert(
        loc=3,
        column='Season',
        value=str(int(season[:4]) - 1) + '-' + season[2:4]
    )

    return df_mvp


@task(
    name="Load MVP Data to S3",
    description="Load MVP data to S3 bucket",
    tags=["NBA", "Basketball-Reference", "MVP", "Ingestion"]
)
def save_mvp_data_to_s3(df_mvp, path):
    """
    Saves MVP data DataFrame to an S3 bucket in Parquet format.
    
    Args:
        df_mvp (pd.DataFrame): The DataFrame containing MVP data.
        path (str): The S3 path to save the Parquet file.
    """
    try:
        wr.s3.to_parquet(
            df=df_mvp,
            path=path,
        )
        print("Data loaded successfully!")
    except Exception as e:
        print(e)


#########################################################
#                   FLOW DEFINITION                     #
#########################################################

@flow(name="MVP Data Scraper", log_prints=True)
def mvp_data_scraper():

    # Create an empty list to store DataFrames
    all_dataframes = []

    # Retrieve MVP data for each season and append to the list
    for season in SEASONS:
        df = get_mvp_data(season=season)
        all_dataframes.append(df)
        time.sleep(1)  # Sleep to avoid overwhelming the server

    # Concatenate all DataFrames in the list into a single DataFrame
    concatenated_df = pd.concat(all_dataframes, ignore_index=True)

    # Save the DataFrame to S3 bucket
    save_mvp_data_to_s3(
        df_mvp=concatenated_df,
        path=BUCKET_PATH.format('data/raw/mvp/mvp.parquet')
    )


#########################################################
#                       RUN FLOW                        #
#########################################################

if __name__ == "__main__":
    # Run flow
    mvp_data_scraper()
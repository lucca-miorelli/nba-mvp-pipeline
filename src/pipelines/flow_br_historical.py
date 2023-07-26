from prefect import Flow
from tasks.tasks_br_scraper import get_advanced_stats_2

@Flow
def historical_data_scraper():

    df = get_advanced_stats_2(season="2010", info="advanced")
    print(df.head())


if __name__ == "__main__":
    historical_data_scraper()

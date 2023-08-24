from prefect.filesystems import GitHub
from prefect.filesystems import S3
from prefect_sqlalchemy import DatabaseCredentials
import boto3
from configparser import ConfigParser

config = ConfigParser()
config.read("credentials.conf")

session = boto3.Session()
credentials = session.get_credentials()
current_credentials = credentials.get_frozen_credentials()

gh = GitHub(
    repository="lucca-miorelli/nba-mvp-pipeline",
    reference="master",
)
gh.save("gh-master", overwrite=True)

s3 = S3(
    bucket_path="nba-mvp-pipeline",
    aws_access_key_id=current_credentials.access_key,
    aws_secret_access_key=current_credentials.secret_key,
)
s3.save("nba-mvp-pipeline", overwrite=True)

db = DatabaseCredentials(
    host=config["POSTGRES"]["HOST"],
    port=config["POSTGRES"]["PORT"],
    database=config["POSTGRES"]["DATABASE"],
    username=config["POSTGRES"]["USER"],
    password=config["POSTGRES"]["PASSWORD"],
    driver="postgresql+psycopg2"
)
db.save("lk-rds-credentials", overwrite=True)


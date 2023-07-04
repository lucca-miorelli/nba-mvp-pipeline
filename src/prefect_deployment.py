from prefect.deployments import Deployment
from prefect_pipeline import nba_etl
from prefect.filesystems import GitHub
import os

GITHUB_BLOCK = GitHub.load("test-gh")

def deploy_nba_etl():
    """
    Deploy the NBA ETL flow to Prefect Cloud
    """

    # Build a deployment from the flow
    deployment = Deployment.build_from_flow(
        flow=nba_etl,
        name="[ETL] NBA Stats Current Season",
        output=os.path.join("src", "nba-etl-deployment.yaml"),
        infrastructure="process",
        storage=GITHUB_BLOCK
        # schedule="*/5 * * * *" 
    )

    # Register the deployment
    deployment.apply()


if __name__ == "__main__":
    deploy_nba_etl()
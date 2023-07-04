#!/usr/bin/env bash

prefect deployment build ".\src\prefect_pipeline.py:nba_etl" --name "NBA 2022-23 Season Stats Scrapper" --tag "etl" --tag "test" --pool default-agent-pool --work-queue default --infra process --storage-block github/test-gh --cron "0 */4 * * *" --output "./src/nba-etl-deployment.yaml" --apply    

# prefect deployment build ".\src\prefect_pipeline.py:nba_etl"    \
#     --name "NBA 2022-23 Season Stats Scrapper"                  \
#     --tag "etl" --tag "test"                                    \
#     --pool default-agent-pool                                   \
#     --work-queue default                                        \
#     --infra process                                             \
#     --storage-block github/test-gh                              \
#     --cron "0 */4 * * *"                                        \
#     --output "./src/nba-etl-deployment.yaml"                    \
#     --apply    


# Agent Start
# prefect agent start --pool default-agent-pool --work-queue default
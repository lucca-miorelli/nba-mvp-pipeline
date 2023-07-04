# MVP Predictor: Predicting NBA's Most Valuable Player

This project aims to to predict the NBA's Most Valuable Player (MVP) for the current season.
This README is a description of how to create, activate, and install all Python requirements in a virtual environment, it also provides instructions on deploying a flow with the Prefect CLI.

## Table of Contents

- [MVP Predictor: Predicting NBA's Most Valuable Player](#mvp-predictor-predicting-nbas-most-valuable-player)
  - [Table of Contents](#table-of-contents)
  - [Virtual Environment Setup](#virtual-environment-setup)
  - [Installing Python Requirements](#installing-python-requirements)
  - [Deploying a Flow with Prefect CLI](#deploying-a-flow-with-prefect-cli)

## Virtual Environment Setup

To manage Python packages and ensure project dependencies are isolated, it is recommended to use a virtual environment. Follow these steps to set up a virtual environment:

1. Install `virtualenv` if you haven't already:

   ```shell
   $ python3 -m venv .venv
   ```

2. Create a new virtual environment:

   ```shell
   $ virtualenv .venv
   ```

3. Activate the virtual environment:

   - For Windows:
     ```shell
     $ .\.venv\Scripts\Activate.ps1
     ```
   
   - For Unix/Linux:
     ```shell
     $ source .venv/bin/activate
     ```

## Installing Python Requirements

Once the virtual environment is set up and activated, proceed to install the Python requirements for this project. Typically, these requirements are specified in a `requirements.txt` file.

1. Navigate to the project directory.

2. Install the requirements:

   ```shell
   $ pip install -r requirements.txt
   ```

## Deploying a Flow with Prefect CLI

To deploy a flow using the Prefect CLI, follow these steps:

1. Ensure that Prefect is installed:

   ```shell
   $ pip install prefect
   ```

2. Authenticate with the Prefect server:

   ```shell
   # This authenticates to an already created Prefect Cloud account
   $ prefect cloud login --key <API_KEY>
   ```

3. Create a flow file (e.g., `flow.py`) with the desired flow definition.

4. Build the flow deployment  configuration (`.yaml`) file:

   ```shell
    $ prefect deployment build ".\flow.py:function"                 \
        --name "Flow Deployment Name"                               \
        --tag "tag-1" --tag "tag-2"                                 \
        --pool default-agent-pool                                   \
        --work-queue default                                        \
        --infra process                                             \
        --storage-block your_prefect_block                          \
        --cron "cron expression"                                    \
        --output "./src/nba-etl-deployment.yaml"
   ```

5. Apply flow deployment:

   ```shell
   $ prefect deployment apply "./src/nba-etl-deployment.yaml"
   ```

For more information on using the Prefect CLI, refer to the [Prefect documentation](https://docs.prefect.io/core/cli/prefect_deployment_start.html).

---

That's it! You have now learned how to create and activate a virtual environment, install Python requirements, and deploy a flow using the Prefect CLI for predicting the NBA's Most Valuable Player. Enjoy the journey of MVP prediction using machine learning!
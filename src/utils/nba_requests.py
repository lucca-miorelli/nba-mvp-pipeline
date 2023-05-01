import requests
import os
import logging
import json
from typing import Union, Dict, Any


class NbaAPI(object):

    """
    For now, this will only support leagueLeaders endpoint.
    """

    def __init__(self):
        self.base_url = 'https://stats.nba.com/stats/{endpoint}'
        self.endpoint = None
        self.parameters = None
        self.headers = None

    def get_request(
            self
            ,endpoint:str
            ,parameters:dict=None
            ,headers:str=None
            ,LeagueID:str=None
            ,PerMode:str=None
            ,Scope:str=None
            ,Season:str=None
            ,SeasonType:str=None
            ,StatCategory:str=None
        ) -> Union[Dict[str, Any], str]:
        """
        Sends a GET request to the specified API endpoint with optional parameters and headers.

        Args:
            endpoint (str): The endpoint to send the request to.
            parameters (dict): Optional dictionary of query string parameters to include in the request.
            headers (str): Optional dictionary of headers to include in the request.
            LeagueID (str): Optional league ID parameter.
            PerMode (str): Optional per mode parameter.
            Scope (str): Optional scope parameter.
            Season (str): Optional season parameter.
            SeasonType (str): Optional season type parameter.
            StatCategory (str): Optional statistic category parameter.

        Returns:
            Union[Dict[str, Any], str]: If the request is successful, returns the response data in JSON format. Otherwise,
            returns an error message.

        Raises:
            RequestError: If the response status code is not 200 (OK).
        
        Example:
            To get the League Leaders for the 2023-23 regular season, use the following code:
            ```
            nba_api = NBA_API()
            endpoint = 'leagueLeaders'
            parameters = dict(
                LeagueID='00',
                PerMode='Totals',
                Scope='S',
                Season='2022-23',
                SeasonType='Regular%20Season',
                StatCategory='PTS'
            )
            data = nba_api.get_request(
                endpoint=endpoint,
                parameters=parameters
            )
            ```
        """

        if parameters:
            self.parameters = parameters
        else:
            self.parameters = dict(
                LeagueID=LeagueID,
                PerMode=PerMode,
                Scope=Scope,
                Season=Season,
                SeasonType=SeasonType,
                StatCategory=StatCategory
            )

        # Adds endpoint to base_url
        base_url = self.base_url.format(endpoint=endpoint)

        # Creates parameter string manually
        parameter_string = '&'.join(
            '{}={}'.format(
                key, ''
                if val is None
                else str(val)
            ) for key, val in self.parameters.items()
        )

        # Concatenates base_url and parameter_string
        url = "{}?{}".format(base_url, parameter_string)


        # GET request to specific endpoint+parameters
        response = requests.get(
            url=url
        )

        try:
            response = requests.get(
                url=url
            )
            response.raise_for_status()  # Raise exception for non-OK response status codes
        except requests.exceptions.RequestException as e:
            raise RequestError(f"Failed to retrieve data from {url}. Error: {e}")

        return response.json()

        
class RequestError(Exception):
    """Custom exception class for request errors."""
    pass
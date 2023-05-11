import requests
import os
import logging
import json
from typing import Union, Dict, Any


class NbaAPI(object):

    """
    WIP: Adapting to support other endpoint calls.
    """

    def __init__(self):
        self.base_url = 'https://stats.nba.com/stats/{endpoint}'
        self.endpoint = None
        self.parameters = None
        self.headers = None


    def get_leaders(
            self
            ,parameters: dict = None
            ,LeagueID: str = None
            ,PerMode: str = None
            ,Scope: str = None
            ,Season: str = None
            ,SeasonType: str = None
            ,StatCategory: str = None
    ) -> Union[str, dict]:
        
        """
        Sends a GET request to the NBA API `leagueLeaders` endpoint and returns the response as JSON.

        Args:
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

        Example:
            To get the League Leaders for the 2023-23 regular season, use the following code:
            ```
            nba_api = NBA_API()
            parameters = dict(
                LeagueID='00',
                PerMode='Totals',
                Scope='S',
                Season='2022-23',
                SeasonType='Regular%20Season',
                StatCategory='PTS'
            )
            data = nba_api.get_leaders(
                parameters=parameters
            )
            ```
        """

        endpoint = 'leagueLeaders'

        if parameters:
            return self._get_request(endpoint, parameters)
        else:
            parameters = dict(
                LeagueID=LeagueID,
                PerMode=PerMode,
                Scope=Scope,
                Season=Season,
                SeasonType=SeasonType,
                StatCategory=StatCategory
            )

            return self._get_request(endpoint, parameters)

    def get_players(
            self
            ,parameters: dict = None
            ,Country: str = ''
            ,DraftPick: str = ''
            ,DraftRound: str = ''
            ,DraftYear: str = ''
            ,Height: str = ''
            ,Historical: str = '1'
            ,LeagueID: str = '00'
            ,Season: str = '2022-23'
            ,SeasonType: str = 'Regular%20Season'
            ,TeamID: str = '0'
            ,Weight: str = ''
    ) -> Union[str,dict]:
        
        """
        THIS API CALL ALWAYS TIMEOUT.
        Need to find other source to this data.
        """

        endpoint = 'playerindex'

        if parameters:
            return self._get_request(endpoint, parameters)
        else:
            parameters = dict(
                Country = Country
                ,DraftPick = DraftPick
                ,DraftRound = DraftRound
                ,DraftYear = DraftYear
                ,Height = Height
                ,Historical = Historical
                ,LeagueID = LeagueID
                ,Season = Season
                ,SeasonType = SeasonType
                ,TeamID = TeamID
                ,Weight = Weight
            )

            return self._get_request(endpoint, parameters)

    def _get_request(
            self
            ,endpoint:str
            ,parameters:dict=None
            ,headers:str=None
        ) -> Union[Dict[str, Any], str]:
        """
        Auxiliar functions that sends a GET request to the specified API endpoint with optional parameters and headers.

        Args:
            endpoint (str): The endpoint to send the request to.
            parameters (dict): Optional dictionary of query string parameters to include in the request.
            headers (str): Optional dictionary of headers to include in the request.

        Returns:
            Union[Dict[str, Any], str]: If the request is successful, returns the response data in JSON format. Otherwise,
            returns an error message.

        Raises:
            RequestError: If the response status code is not 200 (OK).
        """

        self.parameters = parameters

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

        headers = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "pt-PT,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
            "Host": "stats.nba.com",
            "Origin": "https://www.nba.com",
            "Referer": "https://www.nba.com/",
            "sec-ch-ua": '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
        }

        # GET request to specific endpoint+parameters
        response = requests.get(
            url=url
            ,headers=headers
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
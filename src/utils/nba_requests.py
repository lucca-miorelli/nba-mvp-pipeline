import requests
import os
import logging
import json






class NbaAPI(object):

    def __init__(self):
        self.base_url = 'https://stats.nba.com/stats/{endpoint}'
        self.parameters = None
        self.headers = None

        # print(self.base_url)

    def get_request(
            self
            ,endpoint:str
            ,parameters:dict
            ,headers=None) -> str:

        # Fazer log dessa função
        # Fazer try except com exceções
        # Fazer condicionais de resposta

        base_url = self.base_url.format(endpoint=endpoint)
        # self.parameters = parameters

        # print(parameters)

        parameter_string = '&'.join(
            '{}={}'.format(
                key, ''
                if val is None
                else str(val)
            ) for key, val in parameters.items()
        )

        url = "{}?{}".format(base_url, parameter_string)

        # print(base_url)
        # print(endpoint)
        # print(parameter_string)
        # print(url)

        response = requests.get(
            url=url
        )

        logger.debug(
            msg=\
            "Response: {}".format(json.dumps(response.json(), indent=4))
        )



        

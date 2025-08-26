import requests
from pygnon.config import GBFS_BASE_URL

class GBFSCollector:


    def __init__(self):
        self.base_url = GBFS_BASE_URL


    def get_data_feeds(self) -> list:

        response = requests.get(f'{self.base_url}/gbfs.json')

        if response.status_code == 200:
            data = response.json()
            data_feeds = [feed for feed in data['data']['en']['feeds']]
            return data_feeds

        else:
            print(response.status_code)
            return None

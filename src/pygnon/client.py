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


    def get_gbfs_data(self):

        try:
            gbfs_data = {}

            for feed in self.get_data_feeds():
                feed_name = feed['name']
                feed_url = feed['url']
                feed_response = requests.get(feed_url)

                if feed_response.status_code == 200:
                    feed_gbfs_data = feed_response.json()
                    gbfs_data[feed_name] = feed_gbfs_data

                else:
                    print(feed_response.status_code)
                    gbfs_data[feed_name] = {}

            return gbfs_data

        except:
            print("GBFS data could not be retrieved.")
            return None

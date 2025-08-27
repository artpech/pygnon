import json
import os
import requests
from pygnon.config import GBFS_BASE_URL, DATA_PATH

class GBFSCollector:


    def __init__(self, load_lattest_gbfs = True):
        self.base_url = GBFS_BASE_URL
        if load_lattest_gbfs:
            self.gbfs_data = self.get_gbfs_data()
        else:
            self.gbfs_data = {}


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
                    print(f"{feed_name} data could not be retrieved")
                    print(f"Status code of the response: {feed_response.status_code}")
                    gbfs_data[feed_name] = {}

            return gbfs_data

        except:
            print("GBFS data could not be retrieved.")
            return None


    def save_to_json(self):
        """
        Saves GBFS data to a JSON file with a timestamp in the name.
        """
        timestamp = self.gbfs_data['gbfs']['last_updated']
        save_dir = os.path.join(DATA_PATH, 'gbfs_json')
        os.makedirs(save_dir, exist_ok = True)

        filename = f'gbfs_data_{timestamp}.json'
        filepath = os.path.join(save_dir, filename)

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.gbfs_data, f, indent=2)

        print(f"Saved file : {filepath}")

        return filepath


    def load_json(self, timestamp: int):
        """
        Load GBFS data from a JSON file with a specific timestamp
        """
        filename = f"gbfs_data_{str(timestamp)}.json"
        load_dir = os.path.join(DATA_PATH, 'gbfs_json')
        filepath = os.path.join(load_dir, filename)

        if os.path.isfile(filepath):

            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)

            self.gbfs_data = data

        else:
            print(f"The file was not loaded. There is no such file as {filepath}.")

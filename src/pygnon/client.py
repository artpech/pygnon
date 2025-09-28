import json
import os
import requests
import time

from datetime import datetime, timedelta
import pandas as pd

from pygnon.config import GBFS_BASE_URL, DATA_PATH
from pygnon.utils import add_vehicle_type_count


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


    def gbfs_collection(self, interval_minutes: int = 1, length_minutes = None):

        start_time = datetime.now()
        interval_seconds = interval_minutes * 60

        if length_minutes:
            length_seconds = length_minutes * 60
            time_count_seconds = 0
            end_message = f"{length_minutes} minute(s)"

        else:
            end_message = "Neverending data collection"

        print(f"""
              🚲 ... Start GBFS data collection ...
              🎬 Start time: {start_time}
              ⏱️ Interval: {interval_minutes} minute(s)
              ⏳ Collection length: {end_message}

              """)

        condition = True

        if end_message:
            print(end_message)

        while condition:

            self.gbfs_data = self.get_gbfs_data()
            self.save_to_json()

            time.sleep(interval_seconds)

            if length_minutes:
                time_count_seconds += interval_seconds
                condition = time_count_seconds < length_seconds

        end_time = datetime.now()

        print(f"""
              Data collection ended at: {end_time}
              """)


    def get_vehicle_types_df(self):
        """Returns a dataframe with the vehicle types data"""
        if self.gbfs_data:
            vehicle_types_df = pd.json_normalize(
                self.gbfs_data['vehicle_types']['data']['vehicle_types'],
                sep = '_'
                )
            return vehicle_types_df
        else:
            raise Exception("No gbfs data")


    def get_station_status_df(self):
        """Returns a dataframe with the station status data"""

        if self.gbfs_data:
            station_status_df = pd.json_normalize(
                self.gbfs_data['station_status']['data']['stations'],
                sep = "_"
                )

            vehicle_types_ls = ['1', '2', '4', '5', '6', '7', '10', '14', '15']
            for vt in vehicle_types_ls:
                station_status_df[f"count_vehicle_type_{vt}"] = station_status_df.apply(
                    lambda row : add_vehicle_type_count(row, vt),
                    axis = 1
                )

            station_status_df.drop(columns = ['vehicle_types_available'], inplace = True)
            station_status_df['timestamp'] = self.gbfs_data['gbfs']['last_updated']

            return station_status_df

        else:
           raise Exception("No gbfs data")


    def get_station_information_df(self):
        """Returns a dataframe with the station information data"""

        if self.gbfs_data:
            stations_info_df = pd.json_normalize(
                self.gbfs_data['station_information']['data']['stations'],
                sep = '_'
                )
            return stations_info_df

        else:
           raise Exception("No gbfs data")


    def get_free_bikes_status_df(self):
        """Returns a dataframe with the free bikes status data"""

        if self.gbfs_data:
            free_bikes_df = pd.json_normalize(
                self.gbfs_data['free_bike_status']['data']['bikes'],
                sep = '_'
                )
            free_bikes_df['timestamp'] = self.gbfs_data['gbfs']['last_updated']
            return free_bikes_df

        else:
           raise Exception("No gbfs data")

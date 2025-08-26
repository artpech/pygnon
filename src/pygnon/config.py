import os
from dotenv import load_dotenv

env_path = os.path.join(os.path.dirname(__file__), '..', '..')
load_dotenv(env_path)

GBFS_BASE_URL = "https://gbfs.omega.fifteen.eu/gbfs/2.2/marseille/en"
DATA_PATH = os.getenv("DATA_PATH")

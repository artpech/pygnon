import os

env_path = os.path.join(os.path.dirname(__file__), '..', '..')

GBFS_BASE_URL = 'https://gbfs.omega.fifteen.eu/gbfs/2.2/marseille/en'
DATA_PATH = os.getenv('DATA_PATH')
DATABASE_CONFIG = {
    'database' : os.getenv('DATABASE_NAME'),
    'user' : os.getenv('DATABASE_USER'),
    'host' : os.getenv('DATABASE_HOST'),
    'password' : os.getenv('DATABASE_PASSWORD'),
    'port' : os.getenv('DATABASE_PORT')
    }

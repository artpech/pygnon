import os

env_path = os.path.join(os.path.dirname(__file__), '..', '..')

GBFS_BASE_URL = 'https://gbfs.omega.fifteen.eu/gbfs/2.2/marseille/en'
DATA_PATH = os.getenv('DATA_PATH')

# Database configuration
DB_CONFIG = {
    'db_host' : os.getenv('DB_HOST'),
    'db_port' : os.getenv('DB_PORT'),
    'db_user' : os.getenv('DB_USER'),
    'db_password' : os.getenv('DB_PASSWORD'),
    'db_name' : os.getenv('DB_NAME'),
    'db_user' : os.getenv('DB_APP_USER'),
    'db_app_password' : os.getenv('DB_APP_PASSWORD')
}

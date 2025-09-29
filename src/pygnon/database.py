import psycopg2
from pygnon.config import DATABASE_CONFIG

def with_db_connection(func):
    """Instantiate the connection to the database,
    commit the transaction and close the connection"""

    def wrapper(*args, **kwargs):

        try:
            conn = psycopg2.connect(**DATABASE_CONFIG)
            cursor = conn.cursor()
            print("✅ Connected to the database!")

            func(cursor, *args, **kwargs)

            conn.commit()
            cursor.close()
            conn.close()
            print("✅ Transaction completed")

        except Exception as e:
            print(f"❌ Erreur : {e}")

    return wrapper

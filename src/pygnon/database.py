import psycopg2
from psycopg2 import sql
from pygnon.config import DATABASE_CONFIG

from datetime import datetime


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


@with_db_connection
def create_db(cursor, sql_schema: str):
    """Create the database from a SQL Schema / SQL file
    Params:
        sql_schema (str): The SQL file with the schema
    """

    with open(sql_schema, "r") as f:
        instructions = f.read().split(";")[:-1]

    for instruction in instructions:
        if instruction.strip()[:12] == "CREATE TABLE":
            table_name = instruction.strip()[14:].split("(")[0][:-1]
            print(table_name)

        cursor.execute(instruction)



@with_db_connection
def insert_into_timestamps(cursor, value: int):
    """Insert the timestamp into the timestamps table of the database
    Params:
        value (int): The timestamp value of the GBFS file"""
    dt = datetime.fromtimestamp(value)
    cursor.execute(
        sql.SQL("insert into {} values (%s)").format(sql.Identifier('timestamps')),
        (dt,)
    )

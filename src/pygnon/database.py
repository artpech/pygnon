import psycopg2
from psycopg2 import sql
from pygnon.config import DATABASE_CONFIG, DATABASE_SCHEMA

from datetime import datetime


def with_db_connection(func):
    """Instantiate the connection to the database,
    commit the transaction and close the connection"""

    def wrapper(*args, **kwargs):

        try:
            conn = psycopg2.connect(**DATABASE_CONFIG)
            cursor = conn.cursor()
            print("✅ Connected to the database!")

            result = func(cursor, *args, **kwargs)

            conn.commit()
            cursor.close()
            conn.close()
            print("✅ Transaction completed")

            return result

        except Exception as e:
            print(f"❌ Erreur : {e}")

    return wrapper


@with_db_connection
def create_db(cursor, sql_schema: str = DATABASE_SCHEMA):
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
def request_db(cursor, query):
    cursor.execute(query)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    results = {
        'columns' : columns,
        'data' : data
    }
    return results


@with_db_connection
def insert_into_timestamps(cursor, value: int):
    """Insert the timestamp into the timestamps table of the database
    Params:
        value (int): The timestamp value of the GBFS file"""

    cursor.execute(
        sql.SQL("INSERT INTO {} VALUES (%s)").format(sql.Identifier('timestamps')),
        (value,)
    )


@with_db_connection
def insert_into_stations(cursor, rows: list):
    """Insert the new rows into the stations table
    Params:
        rows = List of tuples, of the form
            ('id', 'is_active_station')

    """

    columns = ['id', 'is_active_station']
    column_names = sql.SQL(', ').join(map(sql.Identifier, columns))
    placeholders = sql.SQL(', ').join([sql.Placeholder()] * len(columns))

    query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier('stations'),
        column_names,
        placeholders
    )

    cursor.executemany(query, rows)


@with_db_connection
def update_stations(cursor, rows: list):
    """Update stations with the new values in rows
    Params:
        rows = List of tuples, of the form
            ('id', 'is_active_station')
    """

    query = sql.SQL("UPDATE {} SET {} = %s WHERE {} = %s").format(
        sql.Identifier('stations'),
        sql.Identifier('is_active_station'),
        sql.Identifier('id')
    )

    cursor.executemany(query, [(row[1], row[0]) for row in rows])


@with_db_connection
def insert_into_stations_live(cursor, rows: list):
    """Insert the new rows into the stations_live table
    Params:
        rows = List of Tuples
    """

    columns = [
        'station_id', 'num_bikes_available', 'num_docks_available',
        'is_installed', 'is_renting', 'is_returning', 'last_reported',
        'count_vehicle_type_1', 'count_vehicle_type_2', 'count_vehicle_type_4',
        'count_vehicle_type_5', 'count_vehicle_type_6', 'count_vehicle_type_7',
        'count_vehicle_type_10', 'count_vehicle_type_14',
        'count_vehicle_type_15', 'timestamp'
    ]

    column_names = sql.SQL(', ').join(map(sql.Identifier, columns))
    placeholders = sql.SQL(', ').join([sql.Placeholder()] * len(columns))

    query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier('stations_live'),
        column_names,
        placeholders
    )

    cursor.executemany(query, rows)


@with_db_connection
def insert_into_stations_details(cursor, rows: list):
    pass


@with_db_connection
def insert_into_vehicle_types(cursor, rows: list):
    pass

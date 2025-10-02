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
def get_table_columns(cursor, table_name: str, exclude_auto_id: bool = True) -> list:
    """
    Retrieves the list of columns in the PostgreSQL table.

    Params:
        table_name (str): name of the PostgreSQL table
        exclude_auto_id (bool): If True, excludes IDENTITY (auto-incrementing) columns.

    Returns:
        List of column names
    """

    if exclude_auto_id:
        # Retrieves all columns except IDENTITY
        query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            AND table_schema = 'public'
            AND is_identity = 'NO'
            ORDER BY ordinal_position;
        """

    else:
        # Retrieves all columns
        query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            AND table_schema = 'public'
            ORDER BY ordinal_position;
        """

    cursor.execute(query, (table_name,))
    columns = [row[0] for row in cursor.fetchall()]

    return columns


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
def insert_into_db(cursor, table_name: str, rows: list):

    columns = get_table_columns(table_name)

    column_names = sql.SQL(', ').join(map(sql.Identifier, columns))
    placeholders = sql.SQL(', ').join([sql.Placeholder()] * len(columns))

    query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table_name),
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

import psycopg2
from psycopg2 import sql
from pygnon.config import DATABASE_CONFIG, DATABASE_SCHEMA
from pygnon.client import GBFSCollector

from datetime import datetime

import pandas as pd


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
    """Update 'stations' with the new values in rows
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
def update_vehicle_types(cursor, rows: list):
    """Update 'vehicle_types' with the new values in rows
    Params:
        rows = List of tuples, of the form
            ('id', 'form_factor', 'propulsion_type', 'max_range_meters', 'name')
    """

    query = sql.SQL(
        """
        UPDATE {}
            SET {} = %s,
                {} = %s,
                {} = %s,
                {} = %s
        WHERE {} = %s
        """
        ).format(
            sql.Identifier('vehicle_types'),
            sql.Identifier('form_factor'),
            sql.Identifier('propulsion_type'),
            sql.Identifier('max_range_meters'),
            sql.Identifier('name'),
            sql.Identifier('id')
            )

    row_values = [(row[1], row[2], row[3], row[4], row[0]) for row in rows]
    cursor.executemany(query, row_values)


@with_db_connection
def update_bikes(cursor, rows: list):
    """Update 'bikes' with the new values in rows
    Params:
        rows = List of tuples, of the form
            ('id', 'is_active_bike')
    """

    query = sql.SQL("UPDATE {} SET {} = %s WHERE {} = %s").format(
        sql.Identifier('bikes'),
        sql.Identifier('is_active_bike'),
        sql.Identifier('id')
    )

    cursor.executemany(query, [(row[1], row[0]) for row in rows])


def load_gbfs_timestamps_to_db(gbfs: GBFSCollector):
    """Ingest gbfs data to the table 'timestamps'
    Params:
        gbfs (GBFSCollector): A GBFSCollector instance
    """

    timestamp = gbfs.gbfs_data['gbfs']['last_updated']
    insert_into_db(table_name = 'timestamps', rows = [(timestamp,)])


def load_gbfs_stations_to_db(gbfs: GBFSCollector):
    """Ingest gbfs data to the table 'stations'
    Params:
        gbfs (GBFSCollector): A GBFSCollector instance
    """

    station_info_df = gbfs.get_station_information_df()
    stations_df = station_info_df[['station_id', 'is_active_station']].rename(
        columns = {'station_id' : 'id'})

    # --- I. Query the table 'stations' to compare the current state of the table
    # --- with the data fetched in the API call
    query = "SELECT * FROM stations"
    results = request_db(query)
    table_stations_df = pd.DataFrame(data = results['data'], columns = results['columns'])

    # --- II. Add or Update rows to the table 'stations'
    rows_to_add = []
    rows_to_update = []

    # Add empty station id '' for bikes not in any station, if not already in the table
    not_in_station_row = ('no_station', True)
    if table_stations_df[table_stations_df['id'] == 'no_station'].shape[0] == 0:
        rows_to_add.append(not_in_station_row)

    ids_in_table = set(table_stations_df['id'].unique())
    ids_in_new_data = set(stations_df['id'].unique())

    # CASE 1
    ids_in_both_table = ids_in_table.intersection(ids_in_new_data)

    # --- Case 1.1. Stations marked as inactive in table, but active in new data
    # => update these rows (mark them as active stations)
    condition = (table_stations_df['id'].isin(ids_in_both_table)) \
        & (table_stations_df['is_active_station'] == False)
    ids_rows_to_update = table_stations_df[condition]['id'].unique()
    update_rows = [(station_id, True) for station_id in ids_rows_to_update]
    rows_to_update.extend(update_rows)

    # --- Case 1.2. Stations marked as active in table and in new data
    # => DO NOTHING
    pass

    # CASE 2
    # => add rows to the table (mark them as active stations)
    ids_in_new_data_and_not_in_table = ids_in_new_data.difference(ids_in_table)
    new_rows = [(station_id, True) for station_id in ids_in_new_data_and_not_in_table]
    rows_to_add.extend(new_rows)

    # CASE 3
    # => update rows (mark them as inactive stations)
    ids_in_table_and_not_in_new_data = ids_in_table.difference(ids_in_new_data)
    update_rows = [(station_id, False) for station_id in ids_in_table_and_not_in_new_data]

    # always keep not_in_station_row as an active row
    if not_in_station_row in update_rows:
        update_rows.remove(not_in_station_row)

    rows_to_update.extend(update_rows)

    # III. Perform transactions into the database
    insert_into_db(table_name = 'stations', rows = rows_to_add)
    update_stations(rows_to_update)


def load_gbfs_stations_live_to_db(gbfs: GBFSCollector):
    """Ingest gbfs data to the table 'stations_live'
    Params:
        gbfs (GBFSCollector): A GBFSCollector instance
    """
    col_names = get_table_columns('stations_live')
    station_status_df = gbfs.get_station_status_df()[col_names]
    station_status_list = station_status_df.to_dict(orient = 'records')
    rows = [tuple(ss_dict.values()) for ss_dict in station_status_list]
    insert_into_db(table_name = 'stations_live', rows = rows)


def load_gbfs_stations_details_to_db(gbfs: GBFSCollector):
    """Ingest gbfs data to the table 'stations_details'
    Params:
        gbfs (GBFSCollector): A GBFSCollector instance
    """
    col_names = get_table_columns('stations_details')
    station_details_df = gbfs.get_station_information_df()[col_names]
    station_details_list = station_details_df.to_dict(orient = 'records')
    rows = [tuple(sd_dict.values()) for sd_dict in station_details_list]
    insert_into_db(table_name = 'stations_details', rows = rows)


def load_gbfs_vehicle_types_to_db(gbfs: GBFSCollector):
    """Ingest gbfs data to the table 'vehicle_types'
    Params:
        gbfs (GBFSCollector): A GBFSCollector instance
    """

    #rows_to_add = []
    rows_to_update = []

    # Query the table vehicle_types
    query = "SELECT * FROM vehicle_types"
    results = request_db(query)
    current_rows = results['data']
    current_rows_ids = [row[0] for row in current_rows]

    # Retrieve new row in the gbfs data
    get_vehicle_types_df = gbfs.get_vehicle_types_df().rename(columns = {"vehicle_type_id" : "id"})
    get_vehicle_types_dict = get_vehicle_types_df.to_dict(orient = "records")
    rows = [ tuple(vt.values()) for vt in get_vehicle_types_dict ]

    # Rows already in the table 'vehicle_types' => do nothing
    rows_to_ignore = list(set(rows).intersection(set(current_rows)))
    other_rows = set(rows).difference(rows_to_ignore)

    # Rows whose ids are already in the table 'vehicle_types'
    # but whose values have changed
    # => update rows
    rows_to_update = [row for row in other_rows if row[0] in current_rows_ids]

    # Rows whose ids are not in the table 'vehicle_types'
    rows_to_add = list(other_rows.difference(set(rows_to_update)))

    if rows_to_add:
        insert_into_db(table_name = 'vehicle_types', rows = rows_to_add)

    if rows_to_update:
        update_vehicle_types(rows_to_update)
        pass


def load_gbfs_bikes_to_db(gbfs: GBFSCollector):
    """Ingest gbfs data to the table 'bikes'
    Params:
        gbfs (GBFSCollector): A GBFSCollector instance
    """
    free_bikes_status_df = gbfs.get_free_bikes_status_df()
    bikes_df = free_bikes_status_df[['bike_id', 'is_active_bike']].rename(columns = {'bike_id' : 'id'})

    # --- I. Query the table 'bikes' to compare the current state of the table
    # --- with the data fetched in the API call
    query = "SELECT * FROM bikes"
    results = request_db(query)
    table_bikes_df = pd.DataFrame(data = results['data'], columns = results['columns'])
    table_bikes_df

    # --- II. Add or Update rows to the table 'bikes'
    rows_to_add = []
    rows_to_update = []

    ids_in_table = set(table_bikes_df['id'].unique())
    ids_in_new_data = set(bikes_df['id'].unique())

    # CASE 1
    ids_in_both_table = ids_in_table.intersection(ids_in_new_data)

    # --- Case 1.1. Bikes marked as inactive in table, but active in new data
    # => Update rows and mark them as active bikes
    condition = (table_bikes_df['id'].isin(ids_in_both_table)) & (table_bikes_df['is_active_bike'] == False)
    ids_rows_to_update = table_bikes_df[condition]['id'].unique()
    update_rows = [(bike_id, True) for bike_id in ids_rows_to_update]
    rows_to_update.extend(update_rows)

    # --- Case 1.2. Bikes marked as active in table and in new data
    # => DO NOTHING
    pass

    # CASE 2
    # => add rows to the table (as active bikes)
    ids_in_new_data_and_not_in_table = ids_in_new_data.difference(ids_in_table)
    new_rows = [(bike_id, True) for bike_id in ids_in_new_data_and_not_in_table]
    rows_to_add.extend(new_rows)

    # CASE 3
    # => update rows (as inactive bikes)
    ids_in_table_and_not_in_new_data = ids_in_table.difference(ids_in_new_data)
    update_rows = [(bike_id, False) for bike_id in ids_in_table_and_not_in_new_data]
    rows_to_update.extend(update_rows)

    # III. Perform transactions into the database
    insert_into_db(table_name = 'bikes', rows = rows_to_add)
    update_bikes(rows_to_update)


def load_gbfs_bikes_live_to_db(gbfs: GBFSCollector):
    """Ingest gbfs data to the table 'bikes_live'
    Params:
        gbfs (GBFSCollector): A GBFSCollector instance
    """
    col_names = get_table_columns('bikes_live')
    free_bikes_status_df = gbfs.get_free_bikes_status_df()[col_names]
    bikes_status_list = free_bikes_status_df.to_dict(orient = 'records')
    rows = [tuple(bs_dict.values()) for bs_dict in bikes_status_list]
    insert_into_db(table_name = 'bikes_live', rows = rows)


def load_gbfs_bikes_details_to_db(gbfs: GBFSCollector):
    """Ingest gbfs data to the table 'bikes_details'
    Params:
        gbfs (GBFSCollector): A GBFSCollector instance
    """
    col_names = get_table_columns('bikes_details')
    bikes_details_df = gbfs.get_free_bikes_status_df()[col_names]
    bikes_details_list = bikes_details_df.to_dict(orient = 'records')
    rows = [tuple(bd_dict.values()) for bd_dict in bikes_details_list]
    insert_into_db(table_name = 'bikes_details', rows = rows)


def load_gbfs_to_db(gbfs_file_timestamp: int):
    """Ingest gbfs data to all tables of the database
    Params:
        gbfs (int): The timestamp that identifies the GBFS json file to load into database
    """

    gbfs = GBFSCollector(load_lattest_gbfs = False)
    gbfs.load_json(timestamp = gbfs_file_timestamp)

    timestamps_list = request_db('SELECT timestamp FROM timestamps')['data']
    if gbfs_file_timestamp in timestamps_list:
        raise Exception('❌​ This timestamp is already in the database. No operation was performed.')

    print("...Loading data into 'timestamps'...")
    load_gbfs_timestamps_to_db(gbfs)

    print("...Loading into 'stations'")
    load_gbfs_stations_to_db(gbfs)

    print("...Loading into 'stations_live'")
    load_gbfs_stations_live_to_db(gbfs)

    print("...Loading into 'stations_details'")
    load_gbfs_stations_details_to_db(gbfs)

    print("...Loading into 'vehicle_types'")
    load_gbfs_vehicle_types_to_db(gbfs)

    print("...Loading into 'bikes'")
    load_gbfs_bikes_to_db(gbfs)

    print("...Loading into 'bikes_live'")
    load_gbfs_bikes_live_to_db(gbfs)

    print("...Loading into 'bikes_details'")
    load_gbfs_bikes_details_to_db(gbfs)

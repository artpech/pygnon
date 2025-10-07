import pandas as pd

def add_vehicle_type_count(row: pd.Series, vehicle_type_id: int) -> int:
    """This function is used to flatten vehicle_types_available, a list of dictionaries
    in station_status data

    Params:
        row (pd.Series): The status of one bikes station
        vehicle_type_id (int): The vehicle type

    Returns:
        vehicle_type_count (int): The number of vehicles of this type at the station
    """
    try:
        ls = row['vehicle_types_available']
        ls_single_dct = [dct for dct in ls if dct['vehicle_type_id'] == vehicle_type_id]
        vehicle_type_count = ls_single_dct[0]['count']
        return vehicle_type_count
    except:
        return 0

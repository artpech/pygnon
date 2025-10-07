--timestamps
CREATE TABLE timestamps(
    timestamp BIGINT PRIMARY KEY
);


--STATION RELATED TABLES
--stations
CREATE TABLE stations(
    id VARCHAR(255) NOT NULL PRIMARY KEY,
    is_active_station BOOLEAN NOT NULL
);


--stations_details
CREATE TABLE stations_details(
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    station_id VARCHAR(255) NOT NULL REFERENCES stations(id),
    timestamp_last_updated BIGINT NOT NULL REFERENCES timestamps(timestamp),
    name VARCHAR(255) NOT NULL,
    lat FLOAT(53) NOT NULL,
    lon FLOAT(53) NOT NULL,
    is_virtual_station BOOLEAN NOT NULL,
    capacity BIGINT NOT NULL,
    is_valet_station BOOLEAN NOT NULL,
    is_charging_station BOOLEAN NOT NULL,
    vehicle_type_capacity_1 BIGINT NOT NULL,
    vehicle_type_capacity_2 BIGINT NOT NULL,
    vehicle_type_capacity_4 BIGINT NOT NULL,
    vehicle_type_capacity_5 BIGINT NOT NULL,
    vehicle_type_capacity_6 BIGINT NOT NULL,
    vehicle_type_capacity_7 BIGINT NOT NULL,
    vehicle_type_capacity_10 BIGINT NOT NULL,
    vehicle_type_capacity_14 BIGINT NOT NULL,
    vehicle_type_capacity_15 BIGINT NOT NULL
);


-- stations_live
CREATE TABLE stations_live(
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    station_id VARCHAR(255) NOT NULL REFERENCES stations(id),
    timestamp BIGINT NOT NULL REFERENCES timestamps(timestamp),
    num_bikes_available BIGINT NOT NULL,
    num_docks_available BIGINT NOT NULL,
    is_installed BOOLEAN NOT NULL,
    is_renting BOOLEAN NOT NULL,
    is_returning BOOLEAN NOT NULL,
    last_reported BIGINT NOT NULL,
    count_vehicle_type_1 BIGINT NOT NULL,
    count_vehicle_type_2 BIGINT NOT NULL,
    count_vehicle_type_4 BIGINT NOT NULL,
    count_vehicle_type_5 BIGINT NOT NULL,
    count_vehicle_type_6 BIGINT NOT NULL,
    count_vehicle_type_7 BIGINT NOT NULL,
    count_vehicle_type_10 BIGINT NOT NULL,
    count_vehicle_type_14 BIGINT NOT NULL,
    count_vehicle_type_15 BIGINT NOT NULL
);


--BIKE RELATED TABLES
--vehicle_types
CREATE TABLE vehicle_types(
    id BIGINT PRIMARY KEY,
    form_factor VARCHAR(255) NOT NULL,
    propulsion_type VARCHAR(255) NOT NULL,
    max_range_meters BIGINT NOT NULL,
    name VARCHAR(255) NOT NULL
);


-- bikes
CREATE TABLE bikes(
    id VARCHAR(255) PRIMARY KEY,
    is_active_bike BOOLEAN NOT NULL
);


--bikes_live
CREATE TABLE bikes_live(
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    bike_id VARCHAR(255) NOT NULL REFERENCES bikes(id),
    timestamp BIGINT NOT NULL REFERENCES timestamps(timestamp),
    lat FLOAT(53) NOT NULL,
    lon FLOAT(53) NOT NULL,
    is_reserved BOOLEAN NOT NULL,
    is_disabled BOOLEAN NOT NULL,
    last_reported BIGINT NOT NULL,
    current_range_meters BIGINT NOT NULL,
    station_id VARCHAR(255) NOT NULL REFERENCES stations(id)
);


--bikes_changes
CREATE TABLE bikes_details(
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    bike_id VARCHAR(255) NOT NULL REFERENCES bikes(id),
    timestamp_last_updated BIGINT NOT NULL REFERENCES timestamps(timestamp),
    vehicle_type_id BIGINT NOT NULL REFERENCES vehicle_types(id)
);

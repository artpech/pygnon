# [🚲 pygnon 🚲]
Data Collector for LeVélo, Marseille’s public bike-sharing service

## 1. Project Overview

This project aims to retrieve and store GBFS data from LeVélo, the free-floating bike-sharing network in Marseille, France. The goal is to gather and organize this data in a PostgreSQL database for further analysis.

GBFS is an open data standard for real-time bike-sharing information (https://github.com/MobilityData/gbfs). It provides a standardized way for bike-sharing systems to publish data like real-time bike/dock availability, vehicle types or station locations and capacities.

The GBFS standard uses JSON feeds that are updated frequently, making it easy for apps, trip planners, and researchers to integrate bike-sharing data.

LeVélo publishes its GBFS data at this link (https://gbfs.omega.fifteen.eu/gbfs/2.2/marseille/en/gbfs.json) and refreshes it every minute. In our project, we access this data in real time and store it for later analysis.

## 2. Current Features

It is currently possible to fetch the GBFS data every minute and save it as JSON files in the `./data/gbfs_json` directory. These JSON files can then be imported into a local PostgreSQL database, allowing for storage and querying of the data.

## 3. Prerequisites

Before starting, ensure you have the dependencies installed:

### 3.1. Installation of dependencies with Poetry

1. Clone the project locally: `git clone https://github.com/artpech/pygnon.git`
2. Navigate to the project directory: `cd pygnon`
3. Install dependencies with Poetry: `poetry install`

### 3.2. PostgreSQL database configuration : Database instance setup with Docker

1. Download the PostgreSQL image: `docker pull postgres:15`
2. Launch a database instance:

```bash
docker run --name my-postgres \
-e POSTGRES_PASSWORD=mypassword \
-e POSTGRES_USER=myuser \
-e POSTGRES_DB=mydatabase \
-p 5432:5432 \
-d postgres:15
```

### 3.3. Table creation from SQL schema

Execute the script to create the database from the schema in `./data/database/schema.sql`:

 `poetry run python src/pygnon/database.py create_database`

## 4. Running the project

### 4.1. GBFS files retrieval in JSON format

1. Navigate to the project directory: `cd pygnon`
2. Run the command to retrieve GBFS files in JSON format: `poetry run python src/pygnon/main.py`
3. The JSON files will be stored in the `./data/gbfs_json` directory

### 4.2. Importing JSON files data into PostgreSQL database

Run the command to import data from JSON files into the database: `poetry run python src/pygnon/database.py load_files`

With no additional argument this command line will import all json files located in the `./data/gbfs_json` directory  into the database.

You can also run the command with arguments: `poetry run python src/pygnon/database.py load_files [arg1] [arg2]`

- Option 1)
    - [arg1] set to `-l` or `-latest`: the program will retrieve the latest timestamp already in database and will only import JSON files whose timestamps are later than the most recent timestamp in the database
    - Run: `poetry run python src/pygnon/database.py load_files -l`
    - Or: `poetry run python src/pygnon/database.py load_files -latest`
- Option 2)
    - [arg1] and/or [arg2] are set to the 1st and last timestamp of files to load into the database
    - Running: `poetry run python src/pygnon/database.py load_files 1759839816` will only import JSON files whose timestamp are ≥ `1759839816`
    - Running: `poetry run python src/pygnon/database.py load_files 1759839816 1759840604` will only import JSON files whose timestamp are ≥ `1759839816` and ≤ `1759840604`

### 4.3. Real-time GBFS files retrieval and database feeding

```bash
# Start GBFS data recovery in the background and save terminal output to the nohup.out file.
nohup poetry run python -u src/pygnon/main.py &

# Displays the contents of nohup.out in real time
tail -f nohup.out
```

In another terminal window run:

```bash
# Start the process of importing json files every minute to keep the database up to date.
while true; do
poetry run python src/pygnon/database.py load_files -latest
sleep 60
```

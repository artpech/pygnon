# Levelo Data Collector

## Project Overview
This project aims to retrieve and store GBFS data from Levelo, the free-floating bike-sharing network in Marseille, France. The goal is to gather and organize this data in a local PostgreSQL database for further analysis.

## Current Features

### 1. Data Retrieval
It is currently possible to fetch the GBFS data every minute and save it as JSON files in the `data/gbfs_json` directory.

### 2. Database Integration
These JSON files can be imported into a local PostgreSQL database, allowing for storage and querying of the data.

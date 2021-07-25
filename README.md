# Drivers Coop ETL

## Objective
Goal is to load data from two CSV files and one json file into PostgreSQL database via ETL job.

Files from which data is fetched:
1. Driver's json file: https://tdc-takehome-data-server-zl3yz.ondigitalocean.app/drivers.json
2. Trip's CSV file: https://tdc-takehome-data-server-zl3yz.ondigitalocean.app/trips.csv
3. Charge's CSV file: https://tdc-takehome-data-server-zl3yz.ondigitalocean.app/charges.csv

Tables created in Postgres:
1. drivers
2. trips
3. charges

## TDC Schema
https://app.diagrams.net/#G1UWJngdSQh6sPq0CStWcXk_VD_kqEDxix

## Installation

Python Libraries:
* pip install psycopg2
* pip install pandas
* pip install requests

## How to execute
### create_tables.py
* Establishes connection with postgres
* Creates tdcdb database, drops db if it exists

### sql_queries.py
* Drops table if it exists
* Creates drivers,trips,charges tables
* Consist of insert queries

### main.py
* This file needs to be exectued, which in turn triggers the above files
* This file extracts data from the URL, transform into dataframes and loads to respective tables
* It also displays "drivers who have been churned" and "trips without charges"



  

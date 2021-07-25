import os
import psycopg2
import pandas as pd
import csv
import requests
import json
from sql_queries import *
from create_tables import *


def process_driver_file(cur,conn, filepath):
    """Pre-processing drivers file
    :param cur: string - query being executed
    :param filepath: string - path to file, file being extracted
    :return: nothing - data is being loaded into table
    """
    try:
        # open driver file
        resp = requests.get(filepath)
        data = json.loads(resp.text)

        # Flatten data
        driver_df = pd.json_normalize(data)

        # insert driver record
        for index, row in driver_df.iterrows():
            cur.execute(driver_table_insert, row)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def process_trip_file(cur,conn, filepath):
    """Pre-processing drivers file
    :param cur: string - query being executed
    :param filepath: string - path to file, file being extracted
    :return: nothing - data is being loaded into table
    """

    try:
        # open trip file
        with requests.Session() as s:
            download = s.get(filepath)

            decoded_content = download.content.decode('utf-8')

            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            trip_list = list(cr)
            trip_df = pd.DataFrame(data=trip_list[1:], columns=trip_list[0])


        # insert trip record
        for index, row in trip_df.iterrows():
            cur.execute(trip_table_insert, row)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def process_charge_file(cur,conn, filepath):
    """Pre-processing drivers file
    :param cur: string - query being executed
    :param filepath: string - path to file, file being extracted
    :return: nothing - data is being loaded into table
    """

    try:
        # open charge file
        with requests.Session() as s:
            download = s.get(filepath)

            decoded_content = download.content.decode('utf-8')

            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            charge_list = list(cr)
            charge_df = pd.DataFrame(data=charge_list[1:], columns=charge_list[0])

        # insert charge record
        for index, row in charge_df.iterrows():
            cur.execute(charge_table_insert, row)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)




def main():
    postgres_connect()
    conn = psycopg2.connect(host="127.0.0.1", database="tdcdb", user="postgres", password="1234")
    cur = conn.cursor()

    process_driver_file(cur, conn, filepath='https://tdc-takehome-data-server-zl3yz.ondigitalocean.app/drivers.json')
    process_trip_file(cur, conn, filepath='https://tdc-takehome-data-server-zl3yz.ondigitalocean.app/trips.csv')
    process_charge_file(cur, conn, filepath='https://tdc-takehome-data-server-zl3yz.ondigitalocean.app/charges.csv')
    conn.commit()
    print("Tables successfully loaded in Postgres")
    print("---------------------------------------------")
    print("List of drivers who have churned")
    df_churn = pd.read_sql("""
        select t.driver_id, d.first_name
        from trips t
        inner join drivers d
        on t.driver_id = d.driver_id
        where date(t.dropoff_timestamp)< (cast('2021-07-11' as date) - interval '14 day');
    """, con=conn)

    print(df_churn)

    print("---------------------------------------------")
    print("List of trips without charges")
    df_failed_charges = pd.read_sql("""
        select c.trip_id
        from charges c  
        inner join trips t
        on c.trip_id = t.trip_id
        where amount_cents is NULL or amount_cents = 0;
    """, con=conn)

    print(df_failed_charges)

    conn.close()


if __name__ == "__main__":
    main()
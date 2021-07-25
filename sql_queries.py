
# DROP TABLES

drivers_table_drop = "DROP TABLE IF EXISTS drivers CASCADE;"
trips_table_drop = "DROP TABLE IF EXISTS trips CASCADE;"
charges_table_drop = "DROP TABLE IF EXISTS charges;"

# CREATE TABLES

driver_table_create = ("""CREATE TABLE IF NOT EXISTS drivers
 (driver_id int,
  tlc_number int NOT NULL, 
  first_name varchar(50) NOT NULL, 
  last_name varchar(50) NOT NULL ,
  PRIMARY KEY (driver_id)
 );
 """)


trip_table_create = ("""CREATE TABLE IF NOT EXISTS trips
 (trip_id int,
  driver_id int,
  request_timestamp timestamp NOT NULL, 
  pickup_timestamp timestamp NOT NULL,
  dropoff_timestamp timestamp NOT NULL, 
  base_fare decimal NOT NULL, 
  gratuity decimal ,
  PRIMARY KEY (trip_id),
  FOREIGN KEY (driver_id)
      REFERENCES drivers (driver_id));
 """)

charge_table_create = ("""CREATE TABLE IF NOT EXISTS charges
 (charge_id int,
  trip_id int,
  amount_cents int, 
  PRIMARY KEY (charge_id),
  FOREIGN KEY (trip_id)
      REFERENCES trips (trip_id));
 """)


# INSERT RECORDS

driver_table_insert = ("""INSERT INTO drivers
 (driver_id, tlc_number, first_name, last_name ) 
 VALUES (%s, %s, %s, %s)
 ON CONFLICT DO NOTHING;
""")

trip_table_insert = ("""INSERT INTO trips
 (trip_id, driver_id, request_timestamp, pickup_timestamp,dropoff_timestamp, base_fare, gratuity) 
 VALUES (%s, %s, %s, %s, %s, %s, %s)
 ON CONFLICT DO NOTHING;
""")

charge_table_insert = ("""INSERT INTO charges
 (charge_id, trip_id, amount_cents) 
 VALUES (%s, %s, %s)
 ON CONFLICT DO NOTHING;
""")


# QUERY LISTS

drop_table_queries = [drivers_table_drop, trips_table_drop, charges_table_drop]
create_table_queries = [driver_table_create, trip_table_create, charge_table_create]

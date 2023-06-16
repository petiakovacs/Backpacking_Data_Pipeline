#snowflake_manager.py file

import logging
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from snowflake_connector import SnowflakeConnector

#logging.basicConfig(filename=config["log_file_path_v1"], level=logging.INFO, format=' %(asctime)s %(levelname)s %(message)s')

class SnowflakeManager:
    def __init__(self):
        self.snowflake = SnowflakeConnector()
        self.snowflake.connect()

    def create_database(self, database_name):
        self.snowflake.execute_query(f"CREATE DATABASE IF NOT EXISTS {database_name};")

    def create_schema(self, database_name, schema_name):
        self.snowflake.execute_query(f"USE DATABASE {database_name};")
        self.snowflake.execute_query(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")

    def create_table(self, schema_name, table_name, table_structure):
        self.snowflake.execute_query(f"USE SCHEMA {schema_name};")
        self.snowflake.execute_query(f"CREATE TABLE IF NOT EXISTS {table_name} {table_structure};")

    def load_data(self, table_name, file_path, column_name_mapping):
        df = pd.read_csv(file_path)
        df = df.rename(columns=column_name_mapping)
        success, nchunks, nrows, _ = write_pandas(self.snowflake.con, df, table_name)
        return success, nrows

    def close(self):
        self.snowflake.close()

def main():
    manager = SnowflakeManager()
    manager.create_database("TRAVEL_DATA")
    manager.create_schema("TRAVEL_DATA", "TRAVEL")
    manager.create_table("TRAVEL_DATA.TRAVEL", "PLACES", """
        (
            "ORDER" INTEGER,
            Arrival_Date DATE,
            Nights INTEGER,
            Country VARCHAR,
            City VARCHAR,
            Host_Name VARCHAR,
            Couchsurfing_FLG VARCHAR,
            G_FLG VARCHAR,
            Bike_FLG VARCHAR,
            Gender VARCHAR,
            Hosts_Personality_Point INTEGER,
            Location_Point INTEGER,
            Comfort VARCHAR,
            Comment VARCHAR
        );
    """)
    success, nrows = manager.load_data("PLACES", "data/output_data/places.csv", {
        'Order': 'ORDER',
        'Arrival_Date': 'ARRIVAL_DATE',
        'Nights': 'NIGHTS',
        'Country': 'COUNTRY',
        'City': 'CITY',
        'Host_Name': 'HOST_NAME',
        'Couchsurfing_FLG': 'COUCHSURFING_FLG',
        'G_FLG': 'G_FLG',
        'Bike_FLG': 'BIKE_FLG',
        'Gender': 'GENDER',
        'Hosts_Personality_Point': 'HOSTS_PERSONALITY_POINT',
        'Location_Point': 'LOCATION_POINT',
        'Comfort': 'COMFORT',
        'Comment': 'COMMENT'
    })
    if success:
        logging.info(f"Successfully loaded {nrows} rows into the PLACES table")
    else:
        logging.error("Failed to load data into the PLACES table")
    manager.close()

if __name__ == "__main__":
    main()

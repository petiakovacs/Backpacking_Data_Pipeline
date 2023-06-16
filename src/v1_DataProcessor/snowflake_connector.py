from dotenv import load_dotenv
import os
import snowflake.connector
#snowflake_connector.py file

import logging

#logging.basicConfig(filename=config["log_file_path_v1"], level=logging.INFO, format=' %(asctime)s %(levelname)s %(message)s')

class SnowflakeConnector:
    def __init__(self):
        load_dotenv()
        self.user = os.getenv('SNOWFLAKE_USER')
        self.password = os.getenv('SNOWFLAKE_PASSWORD')
        self.account = os.getenv('SNOWFLAKE_ACCOUNT')
        self.region = os.getenv('SNOWFLAKE_REGION')
        self.role = os.getenv('SNOWFLAKE_ROLE')
        self.warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        self.con = None
        self.cur = None

    def connect(self, database=None, schema=None):
        try:
            self.con = snowflake.connector.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                region=self.region,
                role=self.role,
                warehouse=self.warehouse,
                database=database,
                schema=schema
            )
            self.cur = self.con.cursor()
            logging.info("Connection to Snowflake established successfully.")
        except Exception as e:
            logging.error(f"Error connecting to Snowflake: {e}")

    def execute_query(self, query):
        try:
            self.cur.execute(query)
            logging.info(f"Query executed successfully: {query}")
        except Exception as e:
            logging.error(f"Error executing query: {e}")

    def close(self):
        try:
            self.cur.close()
            self.con.close()
            logging.info("Connection to Snowflake closed successfully.")
        except Exception as e:
            logging.error(f"Error closing Snowflake connection: {e}")

    @property
    def connection(self):
        return self.con
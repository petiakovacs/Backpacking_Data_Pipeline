# snowflake_view_creator.py file

from snowflake_connector import SnowflakeConnector
import logging

# Configure logging
#logging.basicConfig(filename=config["log_file_path_v1"], level=logging.INFO, format=' %(asctime)s %(levelname)s %(message)s')

class ViewCreator:
    def __init__(self):
        # Connecting to Snowflake
        self.snowflake = SnowflakeConnector()
        self.snowflake.connect(database="TRAVEL_DATA", schema="TRAVEL")

    def create_view_spending_per_country(self):
        try:
            self.snowflake.execute_query("""
            CREATE OR REPLACE VIEW spending_per_country AS
            SELECT Country, SUM(In_EUR) AS total_spending
            FROM SPENDING
            GROUP BY Country
            """)
            logging.info("View 'spending_per_country' created successfully.")
        except Exception as e:
            logging.error("Error creating view 'spending_per_country': %s", str(e))

    def create_view_avg_spending_per_category(self):
        try:
            self.snowflake.execute_query("""
            CREATE OR REPLACE VIEW avg_spending_per_category AS
            SELECT Category, AVG(In_EUR) AS avg_spending
            FROM SPENDING
            GROUP BY Category
            """)
            logging.info("View 'avg_spending_per_category' created successfully.")
        except Exception as e:
            logging.error("Error creating view 'avg_spending_per_category': %s", str(e))


    def create_view_nights_per_country(self):
        try:
            self.snowflake.execute_query("""
            CREATE OR REPLACE VIEW nights_per_country AS
            SELECT Country, SUM(Nights) AS total_nights
            FROM PLACES
            GROUP BY Country
            """)
            logging.info("View 'nights_per_country' created successfully.")
        except Exception as e:
            logging.error("Error creating view 'nights_per_country': %s", str(e))

    def create_view_spending_category_per_country(self):
        try:
            self.snowflake.execute_query("""
            CREATE OR REPLACE VIEW spending_category_per_country AS
            SELECT Country, Category, SUM(In_EUR) as total_spending
            FROM SPENDING
            GROUP BY Country, Category
            """)
            logging.info("View 'spending_category_per_country' created successfully.")
        except Exception as e:
            logging.error("Error creating view 'spending_category_per_country': %s", str(e))

    def create_view_spending_over_time(self):
        try:
            self.snowflake.execute_query("""
            CREATE OR REPLACE VIEW spending_over_time AS
            SELECT Date, SUM(In_EUR) AS total_spending
            FROM SPENDING
            GROUP BY Date
            """)
            logging.info("View 'spending_over_time' created successfully.")
        except Exception as e:
            logging.error("Error creating view 'spending_over_time': %s", str(e))

    def create_view_spending_vs_nights(self):
        try:
            self.snowflake.execute_query("""
            CREATE OR REPLACE VIEW spending_vs_nights AS
            SELECT s.City, s.Country, SUM(s.In_EUR) AS total_spending, SUM(p.Nights) AS total_nights
            FROM SPENDING s
            JOIN PLACES p ON s.City = p.City AND s.Country = p.Country
            GROUP BY s.City, s.Country
            """)
            logging.info("View 'spending_vs_nights' created successfully.")
        except Exception as e:
            logging.error("Error creating view 'spending_vs_nights': %s", str(e))


    def create_all_views(self):
        self.create_view_spending_per_country()
        self.create_view_avg_spending_per_category()
        self.create_view_spending_category_per_country()
        self.create_view_spending_over_time()
        self.create_view_nights_per_country()
        self.create_view_spending_vs_nights()

    def close(self):
        self.snowflake.close()
        self.snowflake.close()

def main():
    creator = ViewCreator()
    creator.create_all_views()
    creator.close()

# Running the main function
if __name__ == "__main__":
    main()

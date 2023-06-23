# postgres_create_tables.py
import logging
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date, Float
from sqlalchemy.engine import reflection
from dotenv import load_dotenv
import os

class TableCreator:
    def __init__(self):
        # Load connection details
        load_dotenv()
        db_link = os.getenv('POSTGRES_DB_LINK')

        try:
            self.engine = create_engine(db_link)
            logging.info('Successfully created engine.')
        except Exception as e:
            logging.error(f"Failed to create engine: {e}")

        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)
        self.inspector = reflection.Inspector.from_engine(self.engine)

        # Define the spending table
        self.spending = Table(
            'spending', self.metadata,
            Column('title', String),
            Column('date', Date),
            Column('amount', Float),
            Column('currency', String),
            Column('in_eur', Float),
            Column('category', String),
            Column('payment_method', String),
            Column('city', String),
            Column('country', String),
            Column('comment', String),
            extend_existing=True  # Add this line
        )

        # Define the places table
        self.places = Table(
            'places', self.metadata,
            Column('order', Integer),
            Column('arrival_date', Date),
            Column('nights', Integer),
            Column('country', String),
            Column('city', String),
            Column('host_name', String),
            Column('couchsurfing_flg', String),
            Column('g_flg', String),
            Column('bike_flg', String),
            Column('gender', String),
            Column('hosts_personality_point', Integer),
            Column('location_point', Integer),
            Column('comfort', String),
            Column('comment', String),
            extend_existing=True  # Add this line
        )

    def create_tables(self):
        try:
            # Check if the tables exist
            if not self.inspector.has_table('spending') or not self.inspector.has_table('places'):
                # Create the tables in the database
                self.metadata.create_all(bind=self.engine)
                logging.info('Successfully created tables.')
            else:
                logging.info('Tables already exist. Skipping table creation.')
        except Exception as e:
            logging.error(f"Failed to create tables: {e}")


def main():
    table_creator = TableCreator()
    table_creator.create_tables()


if __name__ == "__main__":
    main()

import logging
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date, Float
from dotenv import load_dotenv
import os

# Set up logging
#logging.basicConfig(filename=config["log_file_path_v2"], level=logging.INFO, format=' %(asctime)s %(levelname)s %(message)s')

# Load connection details
load_dotenv()
db_link = os.getenv('POSTGRES_DB_LINK')


try:
    engine = create_engine(db_link)
    logging.info('Successfully created engine.')
except Exception as e:
    logging.error(f"Failed to create engine: {e}")

metadata = MetaData()

# Define the spending table
spending = Table(
    'spending', metadata,
    Column('title', String),
    Column('date', Date),
    Column('amount', Float),
    Column('currency', String),
    Column('in_eur', Float),
    Column('category', String),
    Column('payment_method', String),
    Column('city', String),
    Column('country', String),
    Column('comment', String)
)

# Define the places table
places = Table(
    'places', metadata,
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
    Column('comment', String)
)

try:
    # Create the tables in the database
    metadata.create_all(engine)
    logging.info('Successfully created tables.')
except Exception as e:
    logging.error(f"Failed to create tables: {e}")

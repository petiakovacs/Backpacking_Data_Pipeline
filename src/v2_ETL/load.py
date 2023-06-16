#load.py file
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd

#logging.basicConfig(filename=config["log_file_path_v2"], level=logging.INFO, format=' %(asctime)s %(levelname)s %(message)s')
load_dotenv()

class DataExportError(Exception):
    """Exception raised when an error occurswhile exporting data."""

class DataLoader:
    def __init__(self, validated_data, output_paths, missing_data, missing_data_output_paths, db_link):
        """Initialize the DataLoader with the given cleaned data, output paths, missing data, and missing data output paths."""
        self.validated_data = validated_data
        self.output_paths = output_paths
        self.missing_data = missing_data
        self.missing_data_output_paths = missing_data_output_paths
        self.db_link = db_link

    def export_data(self, export_validated=False, export_missing=True):
        """Export the cleaned data and missing data to the given output paths."""
        if export_validated:
            # Export validated data to a CSV file
            for name, path in self.output_paths.items():
                try:
                    self.validated_data[name].to_csv(path, index=False)
                    logging.info(f"{name} data exported successfully.")
                except Exception as e:
                    raise DataExportError(f"An error occurred while exporting the {name} data: {e}")
                
        if export_missing:
            # Export missing data to a CSV file
            for name, path in self.missing_data_output_paths.items():
                try:
                    self.missing_data[name].to_csv(path, index=False)
                    logging.info(f"{name} missing data exported successfully.")
                except Exception as e:
                    raise DataExportError(f"An error occurred while exporting the {name} missing data: {e}")
    
    def load_data_to_db(self, schema):
        """Load the cleaned data into the database."""


        try:
            engine = create_engine(self.db_link)
            logging.info('Successfully created engine.')
        except Exception as e:
            logging.error(f"Failed to create engine: {e}")
            raise

        for name, df in self.validated_data.items():
            
            df = pd.DataFrame(df)    
            column_mapping = {
                'Title': 'title',
                'Date': 'date',
                'Amount': 'amount',
                'Currency': 'currency',
                'In EUR': 'in_eur',
                'Category': 'category',
                'Payment Method': 'payment_method',
                'City': 'city',
                'Country': 'country',
                'Comment': 'comment'
            } if name == 'spending' else {
                'Order': 'order',
                'Arrival_Date': 'arrival_date',
                'Nights': 'nights',
                'Country': 'country',
                'City': 'city',
                'Host_Name': 'host_name',
                'Couchsurfing_FLG': 'couchsurfing_flg',
                'G_FLG': 'g_flg',
                'Bike_FLG': 'bike_flg',
                'Gender': 'gender', 
                'Hosts_Personality_Point': 'hosts_personality_point',
                'Location_Point': 'location_point',
                'Comfort': 'comfort',
                'Comment': 'comment'
            }

            df = df.rename(columns=column_mapping)

            try:
                df.to_sql(name+"_temp", engine, if_exists='replace',index=False)
                logging.info(f"Temporary table {name}_temp created successfully.")
            except Exception as e:
                logging.error(f"An error occurred while creating the temporary table: {e}")
                raise  

            try:

                # Define a mapping between DataFrame column names and database column names
                

                # Generate SQL to upsert temp table into actual table
                columns = '"'+'", "'.join(column_mapping.values())+'"'
                insert_sql = f"INSERT INTO {schema}.{name} ({columns}) SELECT {columns} FROM {schema}.{name}_temp AS temp;"

                with engine.connect() as conn:
                    stmt = text(insert_sql)
                    conn.execute(stmt)

                print(insert_sql)
                # Execute the SQL

                logging.info(f"{name} data loaded successfully.")
            except Exception as e:
                logging.error(f"An error occurred while loading the {name} data: {e}")
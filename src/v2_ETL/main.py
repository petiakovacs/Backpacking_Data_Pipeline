#main.py file

import logging
import json
from extract import DataExtractor, DataLoadingError
from transform import DataTransformer, DataValidationError, DataCleaningError
from load import DataLoader, DataExportError
from dotenv import load_dotenv
import os
import pandas as pd

def main():

    # Load the config file
    with open('config.json') as f:
        config=json.load(f)
    logging.basicConfig(filename=config["log_file_path_v2"], level=logging.INFO, format=' %(asctime)s %(levelname)s %(message)s')

    # Load environment variables
    load_dotenv()

    file_paths = {
        'spending' : config['spending_file_path'],
        'places' : config['places_file_path']
    }

    # Use file paths from config file
    extractor = DataExtractor(file_paths=file_paths, bucket= config['s3bucket'])

    # Extract Data
    try:
        data = extractor.extract_data()
    except DataLoadingError as e:
        logging.error(str(e))
        return
    
    # Check Data
    required_cols = {
        'spending' : config['spending_required_cols'],
        'places' : config['places_required_cols']
    }
    transformer = DataTransformer(data=data)
    try:
        missing_data, checked_data = transformer.check_data(required_cols=required_cols)
        transformer.check_missing_data_threshold(missing_data=missing_data,threshold=config['missing_data_threshold'])
    except DataValidationError as e:
        logging.error(str(e))
        return
    
    # Clean data
    try:
        cleaned_data = transformer.clean_data(data=checked_data)
    except DataCleaningError as e:
        logging.error(str(e))
        return
    
    # Validate data
    try:
        validated_data = transformer.validate_data(data=cleaned_data)
    except AssertionError as e:
        logging.error(f"An error occurred while validating the data: {e}")
        return

    # Export cleaned data
    output_paths = {
        'spending' : config['cleaned_spending_output_path'],
        'places' : config['cleaned_places_output_path']
    }

    # Export data to review manually
    missing_data_output_paths = {
        'spending' : config['missing_spending_output_path'],
        'places' : config['missing_places_output_path']
    }

    # Get DB url from the environment variables
    db_link = os.getenv('POSTGRES_DB_LINK')

    # Load validated data / missing data into csv files
    loader = DataLoader(validated_data=validated_data,missing_data=missing_data,output_paths=output_paths, missing_data_output_paths=missing_data_output_paths, db_link=db_link )
    try:
        loader.export_data()
    except DataExportError as e:
        logging.error(str(e))
        return
    
      # Load data into database
    try:
        loader.load_data_to_db(schema='public')
    except Exception as e:
        logging.error(f"An error occurred while loading the data into the database: {e}")
        return

if __name__ == "__main__":
    main()

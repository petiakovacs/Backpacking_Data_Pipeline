#Data_processing.py file

# Import necessary libraries
import pandas as pd
import logging
import json

#logging.basicConfig(filename=config["log_file_path_v1"], level=logging.INFO, format=' %(asctime)s %(levelname)s %(message)s')

class DataProcessor:
    def __init__(self, file_paths, export_missing_data=True):
        self.file_paths = file_paths
        self.export_missing_data = export_missing_data
        self.data = {}
        
    def load_data(self):
        # Load data from Excel files
        for name, path in self.file_paths.items():
            try:
                self.data[name] =  pd.read_excel(path)
                logging.info(f"{name} data loaded successfully.")
            except Exception as e:
                logging.error(f"An error occurred while loading the {name} data: {e}")
                raise e
        
    
    def check_data(self, required_cols):
        missing_data = {}
        checked_data = {}

        for name, df in self.data.items():
            # Check for nulls in required columns and separate rows with missing coluns
            missing_data[name] = df[df[required_cols[name]].isnull().any(axis=1)]
        
            # Log a warning if any missing values are found
            if not missing_data[name].empty:
                logging.warning(f"Null values found in {name} data")

            # Remove rows with missing values from the original data
            checked_data[name] = df.drop(missing_data[name].index)

        return missing_data, checked_data
    

    # Check missing data percentage
    def check_missing_data_threshold(self, missing_data, threshold):
        for name, df in missing_data.items():
            
            # IF there are missing data
            if not df.empty:
                missing_proportion = len(df) / len(self.data[name])
                logging.warning(f"Missing data proportion on {name}: {missing_proportion}")

                # Check if missing data exceeds the threshold
                if missing_proportion > threshold:
                    raise ValueError(f"Proportion of missing data in {name} exceeds threshold({threshold}): {missing_proportion}")


    def clean_data(self, data):
        try:
            # Perform necessary data cleaning tasks
            for name, df in data.items():
                df = df.copy() # Create a copy of the DataFrame
                if name == 'spending':
                # Spending data cleaning
                    df['Date'] = pd.to_datetime(df['Date'])
                    df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')
                    df['Title'] = df['Title'].str.strip()
                if name == 'places':
                # Places data cleaning
                    df['Arrival_Date'] = pd.to_datetime(df['Arrival_Date'])
                    df['Nights'] = pd.to_numeric(df['Nights'], errors='coerce')
                data[name] = df
            logging.info("Data cleaned successfully.")
        except Exception as e:
            logging.error(f"An error occurred while cleaning the data: {e}")
            raise e
        return data

    # Export the cleaned and missing data
    def export_data(self, cleaned_data, output_paths, missing_data, missing_data_output_paths):
        # Export cleaned data to a CSV file
        for name, path in output_paths.items():
            try:
                cleaned_data[name].to_csv(path, index=False)
                logging.info(f"{name} data exported successfully.")
            except Exception as e:
                logging.error(f"An error occurred while exporting the {name} data: {e}")

        # Export missing data to a CSV file if required
        if self.export_missing_data:
            for name, path in missing_data_output_paths.items():
                try:
                    missing_data[name].to_csv(path, index=False)
                    logging.info(f"{name} missing data exported successfully.")
                except Exception as e:
                    logging.error(f"An error occurred while exporting the {name} missing data: {e}")

    # Function to simplify data processing from the other files
    def process_data(self, required_cols, threshold):
        """
        Load, check and clean data.

        :param required_cols: A dictionary specifying the required columns for each DataFrame.
        :param threshold: The threshold for missing data.
        """
        self.load_data()
        missing_data, checked_data = self.check_data(required_cols=required_cols)
        self.check_missing_data_threshold(missing_data=missing_data, threshold=threshold)
        cleaned_data = self.clean_data(data=checked_data)
       
        return cleaned_data

def main():
    # Load the config file
    with open('config.json') as f:
        config=json.load(f)

    file_paths = {
        'spending' : config['spending_file_path_local'],
        'places' : config['places_file_path_local']
    }

    # Use file paths from config file
    processor = DataProcessor(file_paths=file_paths)

    # Load Data
    try:
        processor.load_data()
    except Exception as e:
        logging.error(f"An error occurred while loading the data: {e}")
        return
    
    # Check Data
    required_cols = {
        'spending' : config['spending_required_cols'],
        'places' : config['places_required_cols']
    }
    try:
        missing_data, checked_data = processor.check_data(required_cols=required_cols)
        processor.check_missing_data_threshold(missing_data=missing_data,threshold=config['missing_data_threshold'])
    except ValueError as e:
        logging.error(f"An error occurred while checking the data: {e}")
        return
    
    # Clean data
    try:
        cleaned_data = processor.clean_data(data=checked_data)
    except Exception as e:
        logging.error(f"An error occurred while cleaning the data: {e}")
        return
    
    # Cleaned data output
    output_paths = {
        'spending' : config['cleaned_spending_output_path'],
        'places' : config['cleaned_places_output_path']
    }

    # Export data to review manually
    missing_data_output_paths = {
        'spending' : config['missing_spending_output_path'],
        'places' : config['missing_places_output_path']
    }

    # Export data
    try:
        processor.export_data(cleaned_data=cleaned_data,missing_data=missing_data,output_paths=output_paths, missing_data_output_paths=missing_data_output_paths )
    except Exception as e:
        logging.error(f"An error occurred while exporting the data: {e}")
        return

if __name__ == "__main__":
    main()

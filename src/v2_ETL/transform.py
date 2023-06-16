#transform.py
import logging
import pandas as pd

#logging.basicConfig(filename=config["log_file_path_v2"], level=logging.INFO, format=' %(asctime)s %(levelname)s %(message)s')

class DataValidationError(Exception):
    """Exception raised when the data validation checks fail."""

class DataCleaningError(Exception):
    """Exception raised when an error occurs while cleaning data."""

class DataTransformer:
    def __init__(self, data):
        """Initialize the DataTransformer with the given data."""
        self.data = data

    def check_data(self, required_cols):
        """Check for missing data in the required columns."""
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

    def check_missing_data_threshold(self, missing_data, threshold):
        """Check if the proportion of missing data exceeds the given threshold."""
        for name, df in missing_data.items():
            # IF there are missing data
            if not df.empty:
                missing_proportion = len(df) / len(self.data[name])
                logging.warning(f"Missing data proportion on {name}: {missing_proportion}")

                # Check if missing data exceeds the threshold
                if missing_proportion > threshold:
                    raise DataValidationError(f"Proportion of missing data in {name} exceeds threshold({threshold}): {missing_proportion}")
    
    def validate_data(self, data):
        """Validate the data by performing various checks."""
        for name, df in data.items():
            df = df.copy() # Create a copy of the DataFrame

            # Check data types
            if name == 'spending':
                assert df['Date'].dtype == 'datetime64[ns]', "Date column is not of type datetime"
                assert df['Amount'].dtype in ['int64', 'float64'], "Amount column is not numeric"
            if name == 'places':
                assert df['Arrival_Date'].dtype == 'datetime64[ns]', "Arrival_Date column is not of type datetime"
                assert df['Nights'].dtype in ['int64', 'float64'], "Nights column is not numeric"

            # Check value ranges
            if name == 'places':
                assert df['Nights'].min() >= 0, "Nights column contains negative values"

            # Check for duplicates
            assert df.duplicated().sum() == 0, "Data contains duplicate rows"

        logging.info("Data validated successfully.")
        return data
        
    
    def clean_data(self, data):
        """Clean the data by performing necessary data cleaning tasks."""
        def convert_date(value):
            try:
                return pd.to_datetime(value)
            except ValueError:
                try:
                    return pd.to_datetime(value, format='%Y.%m.%d.')
                except ValueError:
                    return pd.NaT

        try:
            for name, df in data.items():
                df = df.copy() # Create a copy of the DataFrame
                if name == 'spending':
                # Spending data cleaning
                    df['Date'] = df['Date'].apply(convert_date)
                    df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')
                    df['Title'] = df['Title'].str.strip()
                if name == 'places':
                # Places data cleaning
                    df['Arrival_Date'] = df['Arrival_Date'].apply(convert_date)
                    df['Nights'] = pd.to_numeric(df['Nights'], errors='coerce')

                # Update the DataFrame in the data dictionary
                data[name] = df

            logging.info("Data cleaned successfully.")
        except Exception as e:
            raise DataCleaningError(f"An error occurred while cleaning the data: {e}")

        return data

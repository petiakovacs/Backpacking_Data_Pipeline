# v2 Version Project Documentation

## Overview

The second version of the project is a streamlined data pipeline that includes data extraction from **AWS S3**, data transformation (cleaning and validation), and loading the data into a **AWS RDS PostgreSQL** database. The pipeline is divided into several Python scripts, each handling a specific part of the pipeline.

## Data Extraction (`extract.py`)

This script is responsible for loading data from AWS S3. It uses the `DataExtractor` class to perform these tasks.

### Class: **`DataExtractor`**

#### Parameters:

- `file_paths`: A dictionary containing the file paths of the data to be processed.
- `bucket`: The name of the S3 bucket where the data is stored.

#### Methods:

- `extract_data()`: Loads the data from the specified file paths in S3.

## Data Transformation (`transform.py`)

This script performs data cleaning and validation on the extracted data. It uses the `DataTransformer` class to perform these tasks.

### Class: **`DataTransformer`**

#### Parameters:

- `data`: The extracted data.

#### Methods:

- `check_data(required_cols)`: Checks for missing data in the required columns and separates rows with missing data.
- `check_missing_data_threshold(missing_data, threshold)`: Checks if the proportion of missing data exceeds a specified threshold.
- `validate_data(data)`: Validates the data by performing various checks.
- `clean_data(data)`: Cleans the data by performing necessary data cleaning tasks.

## Data Loading (`load.py`)

This script loads the cleaned and validated data into a PostgreSQL database. It uses the `DataLoader` class to perform these tasks.

### Class: **`DataLoader`**

#### Parameters:

- `validated_data`: The cleaned and validated data.
- `output_paths`: The output paths for the cleaned data.
- `missing_data`: The missing data.
- `missing_data_output_paths`: The output paths for the missing data.
- `db_link`: The database connection string.

#### Methods:

- `export_data(export_validated=False, export_missing=True)`: Exports the cleaned and missing data to the given output paths.
- `load_data_to_db(schema)`: Loads the cleaned data into the database.

## Main Script (`main.py`)

This is the main script that runs the entire pipeline. It calls the main function from each of the other scripts in order. If an error occurs during the execution of any script, it is logged and the pipeline is halted.

### **Error Handling**: 

The script includes error handling for each step of the pipeline. If an error is raised during the execution of a script, it is caught, logged, and the pipeline is stopped.

### **Execution**: 

The script executes each step of the pipeline in order, calling the main function from each script. If a script returns an error, the pipeline is halted and the error is logged.

## **Error Handling**

Throughout the pipeline, exceptions are used to handle errors. For example, if data loading fails, a `DataLoadingError` is raised. If data cleaning fails, a `DataCleaningError` is raised. These exceptions are caught and logged, and the pipeline is stopped.

## **Logging**

Logging is used throughout the pipeline to track the progress of the pipeline and to record any errors that occur. The logs are written to a file specified by the `log_file_path_v2` parameter specified in the config file.

## **Data**

The data used in this pipeline comes from two sources:

- *`spending`*: This data includes information about spending, such as the date, amount, currency, category, payment method, city, country, and comments. The data is loaded from an Excel file stored in an AWS S3 bucket.

- *`places`*: This data includes information about places visited, such as the order of visit, arrival date, nights spent, country, city, host name, flags for couchsurfing, gender, host's personality point, location point, comfort, and comments. The data is loaded from an Excel file stored in an AWS S3 bucket.

## **Data Cleaning and Validation**

The data is cleaned and validated using the DataTransformer class. The cleaning process involves:

- Checking for *missing data* in required columns.
- Checking if the proportion of *missing data exceeds a given threshold*.
- Converting *date columns* to datetime format.
- Converting *numeric* columns to numeric format.
- *Stripping* leading and trailing spaces from string columns.

The validation process involves:

- Checking the *data types* of columns.
- Checking the *value ranges* of numeric columns.
- Checking for *duplicate* rows.

## **Data Loading**

The cleaned and validated data is loaded into a `PostgreSQL` database using the `DataLoader` class. The data is loaded into two tables: *`spending`* and *`places`*. The schema for these tables is defined in the `postgres_create_tables.py` script.

The DataLoader class also exports the `cleaned and missing data` to CSV files for manual review.

## **Environment Variables**

The pipeline uses `environment variables` to store sensitive information, such as AWS and PostgreSQL credentials. These environment variables are loaded using the `dotenv` package.

## **Configuration**

The pipeline uses a `config.json` file to store configuration settings, such as file paths, column names, and thresholds for missing data. This file is loaded at the start of the pipeline.

## **Running the Pipeline**

The pipeline is run by executing the `main.py` script. This script calls the main function from each of the other scripts in order. If an error occurs during the execution of any script, it is logged and the pipeline is halted.

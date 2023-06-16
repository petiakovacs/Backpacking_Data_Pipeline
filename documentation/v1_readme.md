# v1 Version Project Documentation

## Overview

This project is a comprehensive data pipeline that includes data processing, analysis, visualization, and loading the data into a Snowflake database. The pipeline is divided into several Python scripts, each handling a specific part of the pipeline.

## Data Processing (`data_processing.py`)

This script is responsible for loading, checking, and cleaning the data. It uses the `DataProcessor` class to perform these tasks.

### Class: `DataProcessor`

#### Parameters:

- `file_paths`: A dictionary containing the file paths of the data to be processed.
- `export_missing_data`: A boolean indicating whether to export rows with missing data.

#### Methods:

- `load_data()`: Loads the data from the specified file paths.
- `check_data(required_cols)`: Checks for missing data in the required columns and separates rows with missing data.
- `check_missing_data_threshold(missing_data, threshold)`: Checks if the proportion of missing data exceeds a specified threshold.
- `clean_data(data)`: Cleans the data by performing necessary data cleaning tasks.
- `export_data(cleaned_data, output_paths, missing_data, missing_data_output_paths)`: Exports the cleaned and missing data to specified output paths.
- `process_data(required_cols, threshold)`: A high-level function that loads, checks, and cleans the data.

## Data Analysis (`data_analyzer.py`)

This script performs analysis on the cleaned data. It uses the `DataAnalyzer` class to perform these tasks.

### Class: `DataAnalyzer`

#### Parameters:

- `cleaned_spending_data_path`: The file path of the cleaned spending data.
- `cleaned_places_data_path`: The file path of the cleaned places data.

#### Methods:

- `load_data()`: Loads the cleaned data from the specified file paths.
- `perform_analysis()`: Performs analysis on the spending and places data and returns a dictionary of results.

## Data Visualization (`data_visualizer.py`)

This script creates visualizations based on the cleaned and analyzed data. It uses the `DataVisualizer` class to perform these tasks.

### Class: `DataVisualizer`

#### Parameters:

- `cleaned_spending_data_path`: The file path of the cleaned spending data.
- `cleaned_places_data_path`: The file path of the cleaned places data.
- `vizualization_folder`: The directory where the plots will be saved.
- `figsize`: The size of the figures to be created.

#### Methods:

- `load_data()`: Loads the cleaned data from the specified file paths.
- `create_spending_distribution_plot()`: Creates a plot showing the distribution of spending.
- `create_spending_by_category_plot()`: Creates a plot showing spending by category.
- `create_spending_vs_nights_plot()`: Creates a scatter plot showing spending vs nights.

## Snowflake Connector (`snowflake_connector.py`)

This script establishes a connection to a Snowflake database using the Snowflake Python connector. It retrieves the Snowflake credentials from environment variables and logs whether the connection was successful or not.

### Class: `SnowflakeConnector`

#### Methods:

- `connect(database=None, schema=None)`: Establishes a connection to the Snowflake database.
- `execute_query(query)`: Executes a SQL query on the Snowflake database.
- `close()`: Closes the connection to the Snowflake database.

## Snowflake Manager (`snowflake_manager.py`)

This script manages the Snowflake database, including creatingtables, loading data, and querying data. It uses the `SnowflakeManager` class to perform these tasks.

### Class: `SnowflakeManager`

#### Parameters:

- `connector`: An instance of the `SnowflakeConnector` class.

#### Methods:

- `create_table(table_name, columns)`: Creates a new table in the Snowflake database.
- `load_data(table_name, data_path)`: Loads data from a specified path into a table in the Snowflake database.
- `query_data(query)`: Executes a SQL query on the Snowflake database and returns the results.

## Snowflake View Creation (`snowflake_view_creator.py`)

This script uses the `SnowflakeConnector` class to create several views in Snowflake, including views of spending per country, average spending per category, nights per country, spending category per country, spending over time, and spending vs nights. This views will be used later in data visualization tools. If an error occurs during view creation, it is logged.

## Main Script (`v1_main.py`)

This is themain script that runs the entire pipeline. It calls the `main` function from each of the other scripts in order. If an error occurs during the execution of any script, it is logged and the pipeline is halted.

- **Configuration**: The script reads from a `config.json` file for configuration settings, such as file paths, column names, and thresholds for missing data. If an error occurs during configuration, it is logged and raised.

- **Environment Variables**: The script uses environment variables for sensitive information like Snowflake credentials. If these environment variables are not set, a KeyError is raised and logged.

- **Error Handling**: The script includes error handling for each step of the pipeline. If an error is raised during the execution of a script, it is caught, logged, and the pipeline is halted.

- **Execution**: The script executes each step of the pipeline in order, calling the `main` function from each script. If a script returns an error, the pipeline is halted and the error is logged.

## Error Handling

Throughout the pipeline, exceptions are used to handle errors. For example, if data loading fails, a `DataLoadingError` is raised. If data cleaning fails, a `DataCleaningError` is raised. These exceptions are caught and logged, and the pipeline is stopped.

## Logging

Logging is used throughout the pipeline to track the progress of the pipeline and to record any errors that occur. The logs are written to a file specified by the `LOG_FILE_PATH` constant.

## Data Validation

Data validation is performed during the data processing step. The `DataProcessor` class checks for missing data in the required columns and separates rows with missing data. It also checks if the proportion of missing data exceeds a specified threshold.

## Data Cleaning

Data cleaning is performed by the `DataProcessor` class. This includes handling missing data, converting data types, and other necessary data cleaning tasks.

## Images

The following image provide a visual representation of the data pipeline:

[Overall Flow of the Project](./img/v1.png)

The image was created using draw.io and provide a clear and concise overview of the project's structure and flow.

## Unit Testing

Unit testing is a level of software testing where individual units/ components of a software are tested. The purpose is to validate that each unit of the software performs as designed. In this project, unit tests are written for each module to ensure that individual parts of the program are correct. The unit tests for this project are located in the `tests` directory. More complex tests will be added in the future.

The test files (`test_data_analyzer.py`, `test_data_processing.py`, `test_data_visualizer.py`) contain unit tests for the `DataAnalyzer`, `DataProcessor`, and `DataVisualizer` classes. The tests use the pytest framework and can be run with the command `pytest`.

## test_data_analyzer.py

This file contains tests for the `DataAnalyzer` class. The `test_perform_analysis` function tests the `perform_analysis` method of the `DataAnalyzer` class. It creates some example data, performs the analysis, and checks that the result is as expected.

## test_data_processing.py

This file contains tests for the `DataProcessor` class. The `test_load_data` function tests the `load_data` method of the `DataProcessor` class. It creates a DataProcessor with file paths to the test Excel files, loads the data, and checks that the data was loaded correctly.

The `test_check_data` function tests the `check_data` method of the `DataProcessor` class. It creates some example data with missing values, checks the data, and checks that the missing data was identified correctly and that the checked data does not contain any missing values.

## test_data_visualizer.py

This file contains tests for the `DataVisualizer` class. The `test_create_spending_distribution_plot` function tests the `create_spending_distribution_plot` method of the `DataVisualizer` class. It creates some example data, initializes the `DataVisualizer` with the data and a directory to save the plot, creates the plot, and checks that the plot was saved correctly.

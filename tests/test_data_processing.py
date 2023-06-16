# test_data_processing.py

import pandas as pd
import pytest
from v1_DataProcessor.data_processor import DataProcessor

def test_load_data():
    # Create a DataProcessor with file paths to the test Excel files
    processor = DataProcessor(file_paths={'spending': 'tests/test_spending_data.xlsx', 'places': 'tests/test_places_data.xlsx'})

    # Load the data
    processor.load_data()

    # Check that the data was loaded correctly
    assert 'spending' in processor.data
    assert isinstance(processor.data['spending'], pd.DataFrame)
    assert 'places' in processor.data
    assert isinstance(processor.data['places'], pd.DataFrame)

def test_check_data():
    # Create some example data with missing values
    data = pd.DataFrame({
        'A': [1, 2, None],
        'B': [4, None, 6]
    })

    # Create a DataProcessor
    processor = DataProcessor(file_paths={})

    # Manually add the example data
    processor.data = {'test': data}

    # Check the data
    missing_data, checked_data = processor.check_data(required_cols={'test': ['A', 'B']})

    # Check that the missing data was identified correctly
    assert len(missing_data['test']) == 2

    # Check that the checked data does not contain any missing values
    assert checked_data['test'].isnull().sum().sum() == 0

# More tests to be added

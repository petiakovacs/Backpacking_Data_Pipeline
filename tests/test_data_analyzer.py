# test_data_analyzer.py

import pandas as pd
import pytest
from src.v1_DataProcessor.data_analyzer import DataAnalyzer

def test_perform_analysis():
    # Create some example data
    spending_data = pd.DataFrame({
        'In EUR': [10, 20, 30],
        'City': ['City1', 'City2', 'City3']
    })
    places_data = pd.DataFrame({
        'Nights': [1, 2, 3],
        'City': ['City1', 'City2', 'City3']
    })

    # Initialize the DataAnalyzer
    analyzer = DataAnalyzer(spending_data, places_data)

    # Perform the analysis
    result = analyzer.perform_analysis()

    # Check that the result is as expected
    assert result == {
        'total_spending': 60,
        'avg_spending': 20,
        'total_nights': 6,
        'avg_nights': 2
    }

# More tests to be added

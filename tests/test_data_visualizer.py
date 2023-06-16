# test_data_visualizer.py

import pandas as pd
import pytest
import matplotlib.pyplot as plt
import os
from src.v1_DataProcessor.data_visualizer import DataVisualizer

def test_create_spending_distribution_plot():
    # Create some example data
    spending_data = pd.DataFrame({
        'In EUR': [10, 20, 30],
        'City': ['City1', 'City2', 'City3']
    })
    places_data = pd.DataFrame({
        'Nights': [1, 2, 3],
        'City': ['City1', 'City2', 'City3']
    })

    # Initialize the DataVisualizer
    visualizer = DataVisualizer(spending_data, places_data, vizualization_folder='tests')

    # Create the plot
    visualizer.create_spending_distribution_plot()

    # Check that the plot was saved correctly
    assert os.path.exists('tests/spending_distribution.png')

# More tests to be added

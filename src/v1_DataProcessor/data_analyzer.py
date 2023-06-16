# Data_analyzer.py file

import pandas as pd
import logging
import json
from typing import Dict, Tuple

#logging.basicConfig(filename=config["log_file_path_v1"], level=logging.INFO, format=' %(asctime)s %(levelname)s %(message)s')

class DataAnalyzer:
    def __init__(self, cleaned_spending_data_path: str, cleaned_places_data_path: str):
        """
        Initialize the DataAnalyzer with spending and places data.

        :param spending_data: The string path to the cleaned spending data.
        :param places_data: The string path to the cleaned places data.
        """
        self.cleaned_spending_data_path = cleaned_spending_data_path
        self.cleaned_places_data_path = cleaned_places_data_path
        self.places_data = pd.DataFrame()
        self.spending_data = pd.DataFrame()

    def load_data(self):
        """
        Perform analysis on the spending and places data.
        """
        self.places_data = pd.read_csv(self.cleaned_places_data_path)
        self.spending_data = pd.read_csv(self.cleaned_spending_data_path)

    def perform_analysis(self) -> Dict[str, float]:
        """
        Perform analysis on the spending and places data.
        """
        # Spending analysis
        total_spending = self.spending_data['In EUR'].sum()
        avg_spending = self.spending_data['In EUR'].mean()

        # Top 5 cities with most spending
        top_cities = self.spending_data.groupby('City')['In EUR'].sum().nlargest(5)

        # Places Analysis
        total_nights = self.places_data['Nights'].sum()
        avg_nights = self.places_data['Nights'].mean()

        # Top 5 cities with most nights spent
        top_stay_cities = self.places_data.groupby('City')['Nights'].sum().nlargest(5)

        logging.info(f"Total spending: {total_spending}")
        logging.info(f"Average spending: {avg_spending}")
        logging.info(f"Top 5 cities with most spending: {top_cities}")

        logging.info(f"Total nights spent: {total_nights}")
        logging.info(f"Average nights spent: {avg_nights}")
        logging.info(f"Top 5 cities with most nights spent: {top_stay_cities}")

        return {
            'total_spending': total_spending,
            'avg_spending': avg_spending,
            'total_nights': total_nights,
            'avg_nights': avg_nights
        }

def main():
    # Load the config file
    with open('config.json') as f:
        config=json.load(f)

    # Initialize the analyser
    analyser = DataAnalyzer(cleaned_spending_data_path=config['cleaned_spending_output_path'],
                            cleaned_places_data_path=config['cleaned_places_output_path'])
    
    analyser.load_data()

    try:
        analyser.perform_analysis()
    except Exception as e:
        logging.error(f"An error occured while performing the analysis: {e}")

if __name__ == "__main__":
    main()

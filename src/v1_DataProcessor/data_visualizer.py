# data_visualizer.py

from typing import Dict, Tuple
import pandas as pd
import logging
import matplotlib.pyplot as plt
import seaborn as sns
import json 

#logging.basicConfig(filename=config["log_file_path_v1"], level=logging.INFO, format=' %(asctime)s %(levelname)s %(message)s')

class DataVisualizer:
    def __init__(self, 
                 cleaned_spending_data_path: str, 
                 cleaned_places_data_path: str,
                 vizualization_folder: str = ".",
                 figsize: Tuple[int,int] = (10,6)):
        """
        Initialize the DataVisualizer with spending and places data.

        :param spending_data: The string path to the cleaned spending data.
        :param places_data: The string path to the cleaned places data.
        :param vizualization_folder: A string representing the directory where the plots will be saved. Defaults to the current directory.
        :param figsize: A tuple representing the size of the figures to be created. Defaults to (10,6).
        """
        self.cleaned_spending_data_path = cleaned_spending_data_path
        self.cleaned_places_data_path = cleaned_places_data_path
        self.vizualization_folder = vizualization_folder
        self.figsize = figsize
        self.spending_data = pd.DataFrame
        self.places_data = pd.DataFrame

    def load_data(self):
        """
        Perform analysis on the spending and places data.
        """
        try:
            self.places_data = pd.read_csv(self.cleaned_places_data_path)
            self.spending_data = pd.read_csv(self.cleaned_spending_data_path)
            logging.info(f"Data loaded successfully")
        except Exception as e:
            logging.error(f"An error occured while loading the data: {e}")
            raise e

    def create_spending_distribution_plot(self):
        """
        Create a plot showing the distribution of spending.
        """
        try:
            plt.figure(figsize=self.figsize)
            sns.histplot(self.spending_data['In EUR'], kde=True)
            plt.title('Distribution of Spending')
            plt.savefig(f"{self.vizualization_folder}/spending_distribution.png")
            plt.close()
            logging.info(f"Distribution of Spending plot created successfully")
        except Exception as e:
            logging.error(f"An error occurred while creating the spending distribution plot: {e}")

    def create_spending_by_category_plot(self):
        """
        Create a plot showing spending by category.
        """
        try:
            plt.figure(figsize=self.figsize)
            self.spending_data.groupby('Category')['In EUR'].sum().plot(kind='bar')
            plt.title('Spending by Category')
            plt.savefig(f"{self.vizualization_folder}/spending_by_category.png")
            plt.close()
            logging.info(f"Spending by Category plot created successfully")
        except Exception as e:
            logging.error(f"An error occurred while creating the spending by category plot: {e}")

    def create_spending_vs_nights_plot(self):
        """
        Create a scatter plot showing spending vs nights.
        """
        try:
            # Merge spending and places data on 'City'
            merged_data = pd.merge(self.spending_data, self.places_data, left_on='City', right_on='City')

            plt.figure(figsize=self.figsize)
            sns.scatterplot(x='Nights', y='In EUR', data=merged_data)
            plt.title('Spending vs Nights')
            plt.savefig(f'{self.vizualization_folder}/spending_vs_nights.png')
            plt.close()
            logging.info(f"Spending vs Nights plot created successfully")

        except Exception as e:
            logging.error(f"An error occurred while creating the spending vs nights plot: {e}")

def main() -> None:
    
    # Load the config file
    with open('config.json') as f:
        config=json.load(f)

    # Initialize
    visualizer = DataVisualizer(cleaned_spending_data_path= config['cleaned_spending_output_path'],
                                cleaned_places_data_path=config['cleaned_places_output_path'],
                                vizualization_folder=config["data_visualization_folder_path"])
    
    # Load the data
    visualizer.load_data()

    # Create visualizations
    visualizer.create_spending_distribution_plot()
    visualizer.create_spending_by_category_plot()
    visualizer.create_spending_vs_nights_plot()


if __name__ == "__main__":
    main()
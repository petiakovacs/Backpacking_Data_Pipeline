import os
import json
import logging

def check_and_create_folders(config_file_path, keyword='path'):
   """
   This function checks for the existence of necessary folders as specified in a configuration file, 
   and creates them if they do not exist. This is useful when a project is pulled from a git repository 
   and the necessary folders are not included in the repository, but are required for the successful 
   execution of the project. 

   This function should be run manually after pulling the project from the git repository and before 
   executing the project.

   Parameters:
   config_file_path (str): The path to the configuration file. The configuration file should be a JSON 
                           file that contains the paths to the necessary folders. The keys containing 
                           the paths should include the word 'path' in them.
   keyword (str): The keyword to look for in the keys of the configuration file. Keys containing this 
                  keyword will be considered as paths. Default is 'path'.

   Returns:
   None
   """
   # Set up logging
   logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
   
   # Check if the config file exists
   if not os.path.isfile(config_file_path):
      logging.error(f"Configuration file {config_file_path} does not exist.")
      return

   # Load json config file
   try:
      with open(config_file_path) as f:
         config = json.load(f)
   except json.JSONDecodeError:
      logging.error(f"Failed to decode JSON from {config_file_path}.")
      return

   # get the folder paths from the config
   folder_paths = []
   for key, value in config.items():
      if keyword in key.lower() and isinstance(value, str):
         folder_paths.append(value)

   # check if each path exists 
   missing_folders = []
   for folder_path in folder_paths:
      # Use os.path to get the directory path
      dir_path = os.path.dirname(folder_path)

      if dir_path and not os.path.exists(dir_path):
         missing_folders.append(dir_path)
      
   missing_folders = set(missing_folders)      
   
   if len(missing_folders) == 0:
      logging.info("All necessary folders are set.")
   else:
      logging.info("The following folders are missing, let's create them:")
      for folder in missing_folders:
         os.makedirs(folder)
         logging.info(f"{folder} created")

def main():
   check_and_create_folders("config.json")

if __name__ == "__main__":
   main()
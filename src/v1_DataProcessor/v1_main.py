# v1_main.py file 
 
import logging
import json
import data_processor, data_analyzer, data_visualizer
import snowflake_manager, snowflake_view_creator


def main () -> None:

   with open("config.json") as f:
      config = json.load(f)
   logging.basicConfig(filename=config["log_file_path_v1"], level=logging.INFO, format=' %(asctime)s %(levelname)s %(message)s')
   
   data_processor.main()
   data_analyzer.main()
   data_visualizer.main()
   
   snowflake_manager.main()
   snowflake_view_creator.main()
   

if __name__ == "__main__":
    main()

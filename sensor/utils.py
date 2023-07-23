from .config import mongo_client
import pandas as pd
import json
import logging

def dump_csv_file_to_mongodb_collection(file_path:str,database_name:str,collection_name:str) -> None:
    try:
        #reading a csv file
        df = pd.read_csv(file_path)
        logging.info(f"Rows and Columns {df.shape}")

        df.reset_index(drop=True,inplace=True)
        json_records = list(json.loads(df.T.to_json()).values())
        mongo_client[database_name][collection_name].insert_many(json_records)
    except Exception as e:
        raise e
    

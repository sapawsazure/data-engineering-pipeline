# src/data_ingestion.py
import pandas as pd
from sqlalchemy import create_engine
import yaml
import os
from dotenv import load_dotenv
from utils import logger

class DataIngestion:
    def __init__(self, config_path):
        self.config = self._load_config(config_path)
        self.source_engine = self._create_source_connection()
        
    def _load_config(self, config_path):
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    
    def _create_source_connection(self):
        load_dotenv()
        conn_string = f"postgresql://{os.getenv('SOURCE_DB_USER')}:{os.getenv('SOURCE_DB_PASSWORD')}@" \
                     f"{self.config['database']['source_db']['host']}:{self.config['database']['source_db']['port']}/" \
                     f"{self.config['database']['source_db']['database']}"
        return create_engine(conn_string)
    
    def extract_data(self, query):
        try:
            logger.info(f"Starting data extraction with query: {query}")
            df = pd.read_sql(query, self.source_engine)
            logger.info(f"Successfully extracted {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Error during data extraction: {str(e)}")
            raise
        
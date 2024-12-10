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
        try:
            db_config = self.config['database']['source_db']
            conn_string = f"postgresql://{db_config['user']}:{db_config['password']}@" \
                         f"{db_config['host']}:{db_config['port']}/" \
                         f"{db_config['database']}"
            return create_engine(conn_string)
        except Exception as e:
            logger.error(f"Error creating database connection: {str(e)}")
            raise

    def extract_data(self, query):
        try:
            logger.info(f"Starting data extraction with query: {query}")
            df = pd.read_sql(query, self.source_engine)
            logger.info(f"Successfully extracted {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Error during data extraction: {str(e)}")
            raise
        
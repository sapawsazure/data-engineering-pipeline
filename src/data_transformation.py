# src/data_transformation.py
import pandas as pd
from utils import logger

class DataTransformation:
    def __init__(self, config):
        self.config = config
    
    def clean_data(self, df):
        """Apply basic cleaning operations to the dataframe"""
        try:
            logger.info("Starting data cleaning process")
            
            # Remove duplicates
            df = df.drop_duplicates()
            
            # Handle missing values
            df = df.fillna(method='ffill')
            
            # Convert date columns
            date_cols = df.select_dtypes(include=['object']).columns
            for col in date_cols:
                try:
                    df[col] = pd.to_datetime(df[col], format=self.config['data_quality']['date_format'])
                except:
                    continue
            
            logger.info("Data cleaning completed successfully")
            return df
        except Exception as e:
            logger.error(f"Error during data cleaning: {str(e)}")
            raise
    
    def transform_data(self, df):
        """Apply business transformations to the data"""
        try:
            logger.info("Starting data transformation")
            
            # Add any derived columns or business logic here
            if 'revenue' in df.columns and 'cost' in df.columns:
                df['profit'] = df['revenue'] - df['cost']
                df['profit_margin'] = (df['profit'] / df['revenue']).round(2)
            
            # Add timestamp for data lineage
            df['etl_timestamp'] = pd.Timestamp.now()
            
            logger.info("Data transformation completed successfully")
            return df
        except Exception as e:
            logger.error(f"Error during data transformation: {str(e)}")
            raise
        
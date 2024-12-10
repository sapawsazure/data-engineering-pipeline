# This script loads data from source to target

from sqlalchemy import create_engine, Table, MetaData, Column, Integer, Date, Numeric
from utils import logger

class DataLoader:
    def __init__(self, config):
        self.config = config
        self.target_engine = self._create_target_connection()
        
    def _create_target_connection(self):
        try:
            db_config = self.config['database']['target_db']
            conn_string = f"postgresql://{db_config['user']}:{db_config['password']}@" \
                         f"{db_config['host']}:{db_config['port']}/" \
                         f"{db_config['database']}"
            return create_engine(conn_string)
        except Exception as e:
            logger.error(f"Error creating target database connection: {str(e)}")
            raise

    def create_target_table(self):
        """Create the target table if it doesn't exist"""
        try:
            metadata = MetaData()
            
            # Define the table structure
            Table('sales_data', metadata,
                  Column('id', Integer, primary_key=True),
                  Column('transaction_date', Date),
                  Column('revenue', Numeric),
                  Column('cost', Numeric),
                  Column('profit', Numeric),
                  Column('profit_margin', Numeric),
                  Column('etl_timestamp', Date)
            )
            
            # Create the table
            metadata.create_all(self.target_engine)
            logger.info("Target table created successfully")
            
        except Exception as e:
            logger.error(f"Error creating target table: {str(e)}")
            raise

    def load_data(self, df):
        """Load the transformed data into the target database"""
        try:
            logger.info("Starting data load process")
            
            # Create target table if it doesn't exist
            self.create_target_table()
            
            # Load data to target database
            df.to_sql('sales_data', 
                     self.target_engine, 
                     if_exists='append', 
                     index=False,
                     method='multi')
            
            logger.info(f"Successfully loaded {len(df)} rows to target database")
            
        except Exception as e:
            logger.error(f"Error during data load: {str(e)}")
            raise
        
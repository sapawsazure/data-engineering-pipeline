# src/data_validation.py
import great_expectations as ge
from utils import logger

class DataValidator:
    def __init__(self, config):
        self.config = config
    
    def validate_data(self, df):
        """Validate the dataframe using Great Expectations"""
        try:
            logger.info("Starting data validation")
            ge_df = ge.from_pandas(df)
            
            # Basic validation suite
            validation_results = ge_df.expect_table_row_count_to_be_between(
                min_value=self.config['data_quality']['row_threshold'],
                max_value=None
            )
            
            # Check for null values
            for column in df.columns:
                validation_results &= ge_df.expect_column_values_to_not_be_null(
                    column,
                    mostly=1 - self.config['data_quality']['null_threshold']
                )
            
            logger.info("Data validation completed")
            return validation_results
        except Exception as e:
            logger.error(f"Error during data validation: {str(e)}")
            raise
        
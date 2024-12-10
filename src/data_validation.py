# src/data_validation.py
from great_expectations.dataset import PandasDataset
from utils import logger

class DataValidator:
    def __init__(self, config):
        self.config = config
    
    def validate_data(self, df):
        """Validate the dataframe using Great Expectations"""
        try:
            logger.info("Starting data validation")
            validation_summary = []
            
            # Convert to Great Expectations dataset
            ge_df = PandasDataset(df)
            
            # Initialize validation results
            all_validations_passed = True
            
            # Validate row count
            row_validation = ge_df.expect_table_row_count_to_be_between(
                min_value=self.config['data_quality']['row_threshold'],
                max_value=self.config['data_quality'].get('max_row_threshold')
            )
            if not row_validation.success:
                validation_summary.append(f"Row count validation failed. Found {row_validation.result['observed_value']} rows, "
                                       f"expected minimum {self.config['data_quality']['row_threshold']}")
                all_validations_passed = False
            else:
                validation_summary.append(f"Row count validation passed. Found {row_validation.result['observed_value']} rows")
            
            # Check for null values in each column
            for column in df.columns:
                null_validation = ge_df.expect_column_values_to_not_be_null(
                    column,
                    mostly=1 - self.config['data_quality']['null_threshold']
                )
                if not null_validation.success:
                    validation_summary.append(f"Null check failed for column {column}")
                    all_validations_passed = False
            
            # Validate numeric columns
            if 'revenue' in df.columns:
                revenue_validation = ge_df.expect_column_values_to_be_between(
                    'revenue',
                    min_value=self.config['data_quality']['validations']['revenue']['min_value'],
                    max_value=self.config['data_quality']['validations']['revenue'].get('max_value')
                )
                if not revenue_validation.success:
                    validation_summary.append("Revenue validation failed: Found values outside expected range")
                    all_validations_passed = False
            
            if 'cost' in df.columns:
                cost_validation = ge_df.expect_column_values_to_be_between(
                    'cost',
                    min_value=self.config['data_quality']['validations']['cost']['min_value'],
                    max_value=self.config['data_quality']['validations']['cost'].get('max_value')
                )
                if not cost_validation.success:
                    validation_summary.append("Cost validation failed: Found values outside expected range")
                    all_validations_passed = False
            
            if 'transaction_date' in df.columns:
                date_validation = ge_df.expect_column_values_to_be_of_type(
                    'transaction_date',
                    'datetime64[ns]'
                )
                if not date_validation.success:
                    validation_summary.append("Transaction date validation failed: Incorrect data type")
                    all_validations_passed = False
            
            # Log validation summary
            if all_validations_passed:
                logger.info("All data validations passed successfully")
                for message in validation_summary:
                    logger.info(message)
            else:
                logger.warning("Some data validations failed")
                for message in validation_summary:
                    logger.warning(message)
                
            return all_validations_passed
            
        except Exception as e:
            logger.error(f"Error during data validation: {str(e)}")
            raise
        
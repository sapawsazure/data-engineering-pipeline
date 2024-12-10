# main.py
from src.data_ingestion import DataIngestion
from src.data_transformation import DataTransformation
from src.data_validation import DataValidator
from utils import logger

def main():
    try:
        # Initialize components
        config_path = "config/config.yaml"
        ingestion = DataIngestion(config_path)
        transformation = DataTransformation(ingestion.config)
        validator = DataValidator(ingestion.config)
        
        # Extract data
        query = """
        SELECT * FROM sales_data 
        WHERE transaction_date >= CURRENT_DATE - INTERVAL '1 day'
        """
        raw_data = ingestion.extract_data(query)
        
        # Transform data
        cleaned_data = transformation.clean_data(raw_data)
        transformed_data = transformation.transform_data(cleaned_data)
        
        # Validate data
        validation_results = validator.validate_data(transformed_data)
        
        if validation_results.success:
            logger.info("Data pipeline completed successfully")
        else:
            logger.error("Data validation failed")
            
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
    
# tests/test_data_pipeline.py
import pytest
import pandas as pd
from src.data_transformation import DataTransformation
from src.data_validation import DataValidator

def test_data_transformation():
    # Test data
    test_data = pd.DataFrame({
        'revenue': [100, 200, 300],
        'cost': [50, 100, 150]
    })
    
    config = {
        'data_quality': {
            'date_format': '%Y-%m-%d'
        }
    }
    
    transformer = DataTransformation(config)
    result = transformer.transform_data(test_data)
    
    assert 'profit' in result.columns
    assert 'profit_margin' in result.columns
    assert result['profit'].iloc[0] == 50
    assert result['profit_margin'].iloc[0] == 0.50

def test_data_validation():
    test_data = pd.DataFrame({
        'col1': range(1000),
        'col2': ['test'] * 1000
    })
    
    config = {
        'data_quality': {
            'row_threshold': 100,
            'null_threshold': 0.1
        }
    }
    
    validator = DataValidator(config)
    results = validator.validate_data(test_data)
    
    assert results.success
    
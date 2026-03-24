import pandas as pd
from src.utils.logger import get_logger

logger = get_logger(__name__)

class DataValidator:
  
  def __init__(self, datasets:dict):
    self.datasets = datasets
  
  #----------------------------------
  #--------SCHEMA VALIDATION---------
  #----------------------------------

  def validate_schema(self, schema_config: dict):
    for dataset_name, expected_columns in schema_config.items():
      df = self.datasets.get(dataset_name)

      if df is None:
        raise ValueError(f'{dataset_name} not found in datasets')

      missing_cols = set(expected_columns) - set(df.columns)
      extra_cols = set(df.columns) - set(expected_columns)

      if missing_cols:
        logger.error(f'{dataset_name}: Missing columns {missing_cols}')
      
      if extra_cols:
        logger.warning(f'{dataset_name}: Extra columns {extra_cols}')

      logger.info(f'{dataset_name}: Schema validation passed')

  def validate_nulls(self, null_rules:dict):
    for dataset_name, columns in null_rules:
      df = self.datasets.get(dataset_name)
    
      for col in columns:
        null_count = df[col].isnull().sum()

        if null_count > 0:
          logger.error(f'{dataset_name}.{col} has {null_count} nulls')
          raise ValueError(f'Null validation failed for {dataset_name}.{col}')
        
      logger.info(f'{dataset_name}: Null validation Passed')

  #---------------------------------
  #------DATA QUALITY RULES---------
  #---------------------------------

  def validate_quality(self):
    # Example Rules - Customize Later

    if 'sales_pipeline' in self.datasets:
      df = self.datasets['sales_pipeline']

      if (df['close_value'] < 0).any():
        raise ValueError('Negative values found in close_value')
      
      if df['deal_stage'].isnull().any():
        raise ValueError('Null deal_stage detected')
      
    if 'sales_teams' in self.datasets:
      df = self.datasets['sales_teams']

      if df['sales_agent'].duplicated().any():
        logger.warning('Duplicate sales agent detected')
      
      logger.info('sales_teams : Quality check passed')

  def run_all_validations(self, schema_config, null_rules):
    logger.info('Starting data validation...')

    self.validate_schema(schema_config)
    self.validate_nulls(null_rules)
    self.validate_quality()

    logger.info('All validations passed successfully')
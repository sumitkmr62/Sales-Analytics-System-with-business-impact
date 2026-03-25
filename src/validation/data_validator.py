import pandas as pd
from src.utils.logger import get_logger

logger = get_logger(__name__)

class DataValidator:
  
  def __init__(self, datasets:dict):
    self.datasets = datasets
    self.errors = []
    self.warnings = []
 
  #----------------------------------
  #--------SCHEMA VALIDATION---------
  #----------------------------------

  def validate_schema(self, schema_config: dict):
    for dataset_name, expected_columns in schema_config.items():
      df = self.datasets.get(dataset_name)

      if df is None:
        msg = f'{dataset_name} not found in datasets'
        logger.error(msg)
        self.errors.append(msg)
        continue

      missing_cols = set(expected_columns) - set(df.columns)
      extra_cols = set(df.columns) - set(expected_columns)

      if missing_cols:
        msg = f'{dataset_name} : Missing columns {missing_cols}'
        logger.warning(msg)
        self.warnings.append(msg)
      
      if extra_cols:
        msg = f'{dataset_name}: Extra columns {extra_cols}'
        logger.warning(msg)
        self.warnings.append(msg)

      logger.info(f'{dataset_name}: Schema validation passed')

  def validate_nulls(self, null_rules:dict):
    for dataset_name, columns in null_rules.items():
      df = self.datasets.get(dataset_name)
    
      if df is None:
        continue

      for col in columns:
        if col not in df.columns:
          msg = f'{col} not found in {dataset_name}'
          logger.error(msg)
          self.errors.append(msg)
          continue

        null_count = df[col].isnull().sum()

        if null_count > 0:
          msg = f'{dataset_name}.{col} has {null_count} nulls'
          logger.warning(msg)
          self.warnings.append(msg)
        
      logger.info(f'{dataset_name}: Null validation Passed')

  #---------------------------------
  #------Business Rules Engine---------
  #---------------------------------

  def validate_business_rules(self, rules_config:dict):

    for dataset_name, rules in rules_config.items():
      df = self.datasets.get(dataset_name)

      if df is None:
        continue 

      for column, rule in rules.items():

        if rule['rule'] == 'not_null_if':
          condition_col = rule['condition_column']
          condition_val = rule['condition_value']
          severity = rule['severity']

          invalid_rows = df[
            (df[condition_col] == condition_val) & (df[column].isnull())
          ]

          if not invalid_rows.empty:
            msg = f'{dataset_name}.{column} has nulls when {condition_col} = {condition_val}'

            if severity == 'error':
              logger.error(msg)
              self.errors.append(msg)
            else:
              logger.warning(msg)
              self.warnings.append(msg)
      logger.info('Business rules validation checked')

  def run_all_validations(self, config):
    logger.info('Starting data validation...')

    self.validate_schema(config['schema'])
    self.validate_nulls(config['null_rules'])

    if 'business_rules' in config:
      self.validate_business_rules(config['business_rules'])

    logger.info('Validation completed')

    return {
      'errors': self.errors,
      'warnings': self.warnings
    }
import pandas as pd
from src.utils.logger import get_logger

logger = get_logger(__name__)

class DataTransformer:
  def __init__(self, datasets: dict):
    self.datasets = datasets

  #----Standardized Columns----
  def standardized_columns(self):
    logger.info('Standardizing column names...')

    if 'sales_teams' in self.datasets:
      df = self.datasets['sales_teams']

      if 'regional_office' in df.columns:
        df = df.rename(columns = {'regional_office':'region'})

      self.datasets['sales_teams'] = df

    logger.info('Column standardization completed.')

  #----Handling Missing Values----
  def handling_missing_values(self):
    logger.info('Handling missing values...')

    if 'sales_pipeline' in self.datasets:
      df = self.datasets['sales_pipeline']

      #----Fill close_value for non won deals
      df['close_value'] = df['close_value'].fillna(0)
      self.datasets['sales_pipeline'] = df

      logger.info('Missing value handling completed.')

  #----Join Datasets----
  def join_datasets(self):
    logger.info('Joining datasets...')

    pipeline = self.datasets['sales_pipeline']
    teams = self.datasets['sales_teams']

    merged = pipeline.merge(
      teams,
      on='sales_agent',
      how='left'
    )

    self.datasets['final_dataset'] = merged

    logger.info('Dataset join completed.')

  def run_all_transformations(self):
    logger.info('Starting transformation pipeline...')

    self.standardized_columns()
    self.join_datasets()
    self.handling_missing_values()

    logger.info('Transformation pipeline completed.')

    logger.info('Final dataset preview: \n%s',self.datasets['final_dataset'].sample(10, random_state=42))

    return self.datasets['final_dataset']
import pandas as pd
from src.utils.logger import get_logger

logger = get_logger(__name__)

def load_csv(file_path):
  try:
    logger.info(f'Loading file: {file_path}')
    df = pd.read_csv(file_path)
    if df.empty:
      raise ValueError('Empty Dataset')
    logger.info(f'Loaded Shape: {df.shape}')
    return df
  except Exception as e:
    logger.error(f'Error Loading File {file_path}: {e}')
    raise
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)

def load_csv(file_path):
  try:
    logging.info(f'Loading file: {file_path}')
    df = pd.read_csv(file_path)
    if df.empty:
      raise ValueError('Empty Dataset')
    logging.info(f'Loaded Shape: {df.shape}')
    return df
  except Exception as e:
    logging.error(f'Error Loading File {file_path}: {e}')
    raise
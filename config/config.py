from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_PATH = BASE_DIR / 'data' / 'raw'

FILES = {
  'accounts':DATA_PATH / 'accounts.csv',
  'products':DATA_PATH / 'products.csv',
  'pipeline':DATA_PATH / 'sales_pipeline.csv',
  'teams':DATA_PATH / 'sales_teams.csv'
}
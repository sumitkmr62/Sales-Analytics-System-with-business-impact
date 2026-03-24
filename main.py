from config.config import FILES
from src.ingestion.load_data import load_csv
from src.utils.logger import get_logger
logger = get_logger(__name__)

def main():
  data = {}

  for name,path in FILES.items():
    data[name] = load_csv(path)

  return data

if __name__ == '__main__':
  datasets = main()


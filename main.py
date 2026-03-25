from src.ingestion.data_loader import load_csv
from src.validation.data_validator import DataValidator
from src.utils.logger import get_logger
from config.config_loader import load_config

logger = get_logger(__name__)

def run_pipeline():
  try:
    logger.info('Pipeline started')

    #----Load config------
    config = load_config()

    #----Ingestion--------
    data = {}
    for name, path in config['data_paths'].items():
      logger.info(f'Loading datasets {name}')
      data[name] = load_csv(path)

    logger.info(f'DATA KEYS: {list(data.keys())}')
    logger.info('Data ingestion completed')

    #----Validation-------
    validator = DataValidator(data)

    result = validator.run_all_validations(config)

    logger.warning(f'Validation warnings: {len(result['warnings'])}')

    if result['errors']:
      logger.error(f'Validation errors: {len(result['errors'])}')
      raise ValueError('Pipeline stopped due to validation error')

    logger.info('Data validation completed')
    
    #----Transformation----
    logger.info('Transformation step (pending)')

    logger.info('Pipeline executed successfully')

    return data
  
  except Exception as e:
    logger.error(f'Pipeline failed {str(e)}')
    raise

if __name__=='__main__':
  run_pipeline()



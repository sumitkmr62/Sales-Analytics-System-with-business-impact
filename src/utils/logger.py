import logging
import os
from datetime import datetime

def get_logger(name:str):
  logger = logging.getLogger(name)

  #Prevent duplicate handlers
  if logger.hasHandlers():
    logger.handlers.clear()
  
  logger.setLevel(logging.INFO)

  #Create logs directory if not exists
  log_dir = 'logs'
  os.makedirs(log_dir, exist_ok=True)

  log_file = os.path.join(log_dir, f'{datetime.now().date()}.log')

  # FIle Handler
  file_handler = logging.FileHandler(log_file)
  file_handler.setLevel(logging.INFO)

  # Console Handler
  console_handler = logging.StreamHandler()
  console_handler.setLevel(logging.INFO)

  # Formatter
  formatter = logging.Formatter(
    '%(asctime)s | %(name)s | %(levelname)s | %(message)s'
  )

  file_handler.setFormatter(formatter)
  console_handler.setFormatter(formatter)

  # Add Handlers
  logger.addHandler(file_handler)
  logger.addHandler(console_handler)

  print("Logger Initialized:", log_file)
  return logger
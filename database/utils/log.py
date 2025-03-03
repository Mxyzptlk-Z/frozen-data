import os
import sys
import logging
from logging.handlers import RotatingFileHandler

class DataLogger:
    """Logger for database to fetch data"""
    
    def __init__(self, log_file_path="datafeed.log"):
        self.log_file_path = log_file_path
        self._init_logger()
    
    def _init_logger(self):
        current_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        log_path = os.path.join(current_path, "logs")
        
        # Create log path if it doesn't exist
        if not os.path.exists(log_path):
            os.makedirs(log_path)
        
        # # Update log file path in the INI configuration
        # config_file = os.path.join(current_path, 'logger_config.ini')
        # with open(config_file, 'r') as file:
        #     config = file.read().replace('log_file_path', os.path.join(log_path, self.log_file_path))
        # # Overwrite config file with new file path
        # with open(config_file, 'w') as file:
        #     file.write(config)
        
        # # Load logging configuration from file
        # logging.config.fileConfig(config_file)


        # Create logger
        root_logger = logging.getLogger()
        frozen_logger = logging.getLogger('frozen')

        # Set logger level
        root_logger.setLevel(logging.INFO)
        frozen_logger.setLevel(logging.INFO)

        # Define logger formatters
        simple_formatter = logging.Formatter(
            fmt='[%(levelname)s] - %(asctime)s - %(name)s - [%(filename)s:%(lineno)d]: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        detailed_formatter = logging.Formatter(
            fmt='[%(process)s:%(threadName)s](%(asctime)s) %(levelname)s - %(name)s - [%(filename)s:%(lineno)d] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(simple_formatter)

        # Create file handler
        log_file = os.path.join(log_path, self.log_file_path) 
        max_size = 5 * 1024 * 1024   # 5 MB
        file_handler = RotatingFileHandler(
            filename=log_file,
            mode="a",
            maxBytes=max_size,
            backupCount=3
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(detailed_formatter)

        # Add handlers to loggers
        root_logger.addHandler(console_handler)
        frozen_logger.addHandler(console_handler)
        frozen_logger.addHandler(file_handler)

        frozen_logger.propagate = False


L = DataLogger()

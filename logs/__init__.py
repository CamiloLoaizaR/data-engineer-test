import logging
import sys


def get_logger(name: str) -> logging.Logger:

    logger = logging.getLogger(name)
    logger.handlers = []

    handler = logging.StreamHandler(sys.stdout)
    
    logger.setLevel(logging.INFO)

    format_logger = logging.Formatter(
        "%(asctime)s - %(name)s - [%(levelname)s] - %(message)s"
    )
    
    handler.setFormatter(format_logger)
    logger.addHandler(handler)
    
    logger.propagate = False

    return logger

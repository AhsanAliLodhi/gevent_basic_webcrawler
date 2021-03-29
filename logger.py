"""Very basic logger."""
import logging

format_string = '%(asctime)s - %(funcName)s - %(lineno)s - %(levelname)s - %(message)s'
logging.basicConfig(
    filename="logs.txt",
    filemode='w+',
    format=format_string,
    level=logging.DEBUG)


def get_logger(log_level_name):
    """Return the singleton logger instance."""
    logger = logging.getLogger(log_level_name)
    return logger

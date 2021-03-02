import os
import json
import logging.config

from config import LOG_CFG, LOG_FILENAME


def custom_logger(default_level='INFO'):
    path = LOG_CFG

    log_level = default_level
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        if LOG_FILENAME:
            config["handlers"]["log_file_handler"]["filename"] = LOG_FILENAME
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=log_level)

    return logging

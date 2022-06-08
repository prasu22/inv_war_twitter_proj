import os
from configparser import ConfigParser

try:
    app_dir = os.environ['APP_DIR']
    config_file_path = f"{app_dir}/config/app_config.ini"
except KeyError:
    ROOT_FOLDER = os.path.dirname(os.path.abspath(__file__))
    config_file_path = f'{ROOT_FOLDER}/config.ini'

APP_CONFIG = ConfigParser(converters={'list': lambda x: [i.strip() for i in x.split(',')]})
APP_CONFIG.read(config_file_path)

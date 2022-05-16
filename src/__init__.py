import logging
import os
from configparser import ConfigParser

from logging.handlers import TimedRotatingFileHandler

try:
    log_dir = f"{os.environ['APP_DIR']}/logs"
except:
    log_dir = "/tmp"

LOG_FILE_NAME = f"{log_dir}/services.log"

logging.basicConfig(
    format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.DEBUG,
    handlers=[ TimedRotatingFileHandler(LOG_FILE_NAME, when='midnight', interval=5, backupCount=5,encoding=None, delay=False,
                             utc=False, atTime=None)])



ROOT_FOLDER = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE_PATH = f'{ROOT_FOLDER}/common/config.ini'
APP_CONFIG = ConfigParser(converters={'list': lambda x: [i.strip() for i in x.split(',')]})
APP_CONFIG.read(CONFIG_FILE_PATH)


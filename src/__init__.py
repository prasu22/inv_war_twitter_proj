import logging
import os

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
    handlers=[TimedRotatingFileHandler(LOG_FILE_NAME, when='D', interval=5, backupCount=5, encoding=None, delay=False,
                                       utc=False, atTime=None)])

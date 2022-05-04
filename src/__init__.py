import logging

from logging.handlers import TimedRotatingFileHandler

LOG_FILE_NAME = "/tmp/var/log/twitter_proj/services.log"
logging.basicConfig(
    format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.DEBUG,
    handlers=[ TimedRotatingFileHandler(LOG_FILE_NAME, when='D', interval=5, backupCount=5,encoding=None, delay=False,
                             utc=False, atTime=None)])

# LOGGER = logging.getLogger(__name__)
# LOGGER.info("data from init file")
import logging
from configparser import ConfigParser

from logging.handlers import TimedRotatingFileHandler

# LOG_FILE_NAME = "/tmp/var/log/twitter_proj/services.log"
# logging.basicConfig(
#     format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
#     datefmt='%Y-%m-%d:%H:%M:%S',
#     level=logging.DEBUG,
#     handlers=[ TimedRotatingFileHandler(LOG_FILE_NAME, when='H', interval=5, backupCount=5,encoding=None, delay=False,
#                              utc=False, atTime=None)])



file = 'common/config.ini'
config = ConfigParser(converters={'list': lambda x: [i.strip() for i in x.split(',')]})
config.read(file)


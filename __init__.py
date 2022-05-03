import os
from configparser import ConfigParser

file = os.getenv("ADDRESS")
config = ConfigParser(converters={'list': lambda x: [i.strip() for i in x.split(',')]})
config.read(file)


from configparser import ConfigParser

file = '../common_variables/config.ini'
config = ConfigParser(converters={'list': lambda x: [i.strip() for i in x.split(',')]})
config.read(file)

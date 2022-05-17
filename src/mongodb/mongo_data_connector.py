from pymongo import MongoClient
import urllib
import logging

from src.common.app_config import APP_CONFIG

LOGGER = logging.getLogger(__name__)

def mongodb_connection():
    """connection with mongodb
       :param
       usr: store the username of database
       pwd: store the password of the database
       server: store the cluser name
       default_db : store the database name
       username = username to access the database
       password = password to access the database
       conn = store the connection detail
    """
    usr = APP_CONFIG.get('mongo', 'username')
    pwd = APP_CONFIG.get('mongo', 'password')
    server = APP_CONFIG.get('mongo', 'servername')
    default_db = APP_CONFIG.get('mongo', 'database')
    username = urllib.parse.quote_plus(usr)
    password = urllib.parse.quote_plus(pwd)
    conn_url = f"mongodb+srv://{username}:{password}@{server}/{default_db}"
    conn = MongoClient(conn_url)
    # print('connect success full',conn)
    return conn


# ======================================================================================================================

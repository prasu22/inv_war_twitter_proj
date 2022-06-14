from pymongo import MongoClient
import urllib
import logging

from src.common.app_config import APP_CONFIG
from src.common.variable_files import DATABASE_TWEET_NEW_DB

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

    try:
        usr = APP_CONFIG.get('mongo', 'username')
        pwd = APP_CONFIG.get('mongo', 'password')
        server = APP_CONFIG.get('mongo', 'servername')
        username = urllib.parse.quote_plus(usr)
        password = urllib.parse.quote_plus(pwd)
        db_location = APP_CONFIG.get('mongo','db_location')
        conn_url=f"{db_location}://{username}:{password}@{server}"
        # print("conn url",conn_url)
        conn = MongoClient(conn_url)
        return conn
    except Exception as e:
        print(f"not connected {e}")


def fetch_data():
    conn = mongodb_connection()
    # conn.drop_database(DATABASE_TWEET_NEW_DB)
    db = conn[DATABASE_TWEET_NEW_DB]
    print(db['tweet_processed_data'].count_documents({}))
fetch_data()

# ======================================================================================================================

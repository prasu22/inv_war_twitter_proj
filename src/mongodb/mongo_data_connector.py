import ssl

from pymongo import MongoClient
from pymongo.errors import CollectionInvalid
import urllib
import logging

from src.common.app_config import APP_CONFIG

LOGGER = logging.getLogger(__name__)


def mongodb_connection():
    """connection with mongodb
       :param
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
    print('connect success full',conn)
    return conn


# ======================================================================================================================

# # ======================================================================================================================
def create_database(db_name, db_collection):
    """creating database and collection schema  with specific schema and indexes
       :param
       con = store the connection details
       db = get the database
       validator = store the fields and requaried property to validate schema
       tweet_schema =  define the schema
       db_collection = give the collection name which we want to create in database
    """
    try:
        con = mongodb_connection()
        db = con[db_name]
        validator = {'$jsonSchema': {'bsonType': 'object', 'properties': {}}}
        required = []
        # defining schema
        tweet_schema = {
            '_id': {
                'type': 'string',
                'required': True,

            },
            'tweet': {
                'type': 'string',
                'required': True
            },
            'country': {
                'type': 'string',
                'required': True
            },
            'date': {
                'type': 'date',
                'required': True
            }

        }

        for field_key in tweet_schema:
            field = tweet_schema[field_key]
            properties = {'bsonType': field['type']}

            if field.get('required') is True:
                required.append(field_key)
            validator['$jsonSchema']['properties'][field_key] = properties

        if len(required) > 0:
            validator['$jsonSchema']['required'] = required

        print("hello i am already present", db.list_collection_names())
        if db_collection not in db.list_collection_names():
            # collection creationg and apply validation on schema
            db.create_collection(db_collection)
            db.command({'collMod': db_collection, 'validator': validator})
            # creation of index in mongodb
            db[db_collection].create_index("country")
            db[db_collection].create_index("date")
            db[db_collection].create_index([('tweet', "text")], default_language='english')
            return db[db_collection]
        return db[db_collection]
    except CollectionInvalid:
        pass


# ======================================================================================================================

# ======================================================================================================================
def connect_with_collection_data():
    """connection with tweet_random collection
       :param
       con = store the connection details
       db_name = database name
       db = mongodb return the database in this variable
    """
    try:
        con = mongodb_connection()
        db_name = "tweet_db"
        db = con[db_name]
        # print(db.list_collection_names())
        if "tweet_data" not in db.list_collection_names():
            return create_database(db, "tweet_data")
        # print(db)
        return db["tweet_data"]
    except Exception as e:
        print(f'Error occured: {e}')
        return "404"
# ======================================================================================================================

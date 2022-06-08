from src.mongodb.mongo_data_connector import mongodb_connection

DATABASE_NAME = "tweet_new_db"
con = mongodb_connection()
db_name = DATABASE_NAME
db = con[db_name]

# collection name
COLLECTION_NAME = "tweet_extract_data"


def update_currency_value(collection_name):
    db[collection_name].update_many({'currency_name': 'NO_Currency'}, {'$set': {'currency_name': 'no currency'}})


def update_country_values(collection_name):
    db[collection_name].update_many({'country': 'Not Found'},
                                    {'$set': {'country': 'no country', 'country_code': 'no country code'}})


def update_collection_name(collection_name):
    db[collection_name].rename("save_raw_data")

# update_currency_value(COLLECTION_NAME)
# update_country_values(COLLECTION_NAME)

# update_collection_name(COLLECTION_NAME)

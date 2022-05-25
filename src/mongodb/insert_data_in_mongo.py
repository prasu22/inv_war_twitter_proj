import logging

from src.common.variable_files import COLL_OF_RAW_DATA, TWEET_KEY
from src.encryption_and_decryption_data.encrypt_and_decryption_data import encrypt_string

LOGGER = logging.getLogger(__name__)

def insert_preprocessed_data(tweet_list,db):
    """
    store the raw data in the mongodb collection
    :param
    tweet_raw_data: initialize the mongodb collection
    """
    print(tweet_list)
    updated_list =[]
    tweet_raw_data = db['test_airflow']

    for message in tweet_list:
        try:
            if message:
                message[TWEET_KEY] = encrypt_string(message[TWEET_KEY])
                updated_list.append(message)
            else:
                LOGGER.info(f"Message:data is not in proper format {message}")
        except Exception as e:
            LOGGER.error(f"Error:{e}")

    tweet_raw_data.insert_many(updated_list)












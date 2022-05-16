import logging

from src.common.variable_files import COLL_OF_RAW_DATA, TWEET_KEY
from src.encryption_and_decryption_data.encrypt_and_decryption_data import encrypt_string

LOGGER = logging.getLogger(__name__)

def insert_preprocessed_data(message,db):
    try:
        tweet_raw_data = db[COLL_OF_RAW_DATA]
        if message:
            message[TWEET_KEY] = encrypt_string(message[TWEET_KEY])
            tweet_raw_data.insert_one(message)
        else:
            LOGGER.info(f"Message:data is not in proper format {message}")
    except Exception as e:
        LOGGER.error(f"Error:{e}")





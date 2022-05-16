import logging

from src.encryption_and_decryption_data.encrypt_and_decryption_data import encrypt_string

LOGGER = logging.getLogger(__name__)

def insert_preprocessed_data(message,db):
    print('data insert successfully')
    try:
        tweet_raw_data = db['tweet_extract_data']
        if message:
            message['tweet'] = encrypt_string(message['tweet'])
            tweet_raw_data.insert_one(message)
        else:
            LOGGER.info(f"Message:data is not in proper format {message}")
    except Exception as e:
        LOGGER.error(f"Error:{e}")





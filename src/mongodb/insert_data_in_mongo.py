import logging
LOGGER = logging.getLogger(__name__)

def insert_preprocessed_data(message,db):
    try:
        tweet_raw_data = db['tweet_extract_data']
        if message:
            tweet_raw_data.insert_one(message)
        else:
            LOGGER.info(f"Message:data is not in proper format {message}")
    except Exception as e:
        LOGGER.error(f"Error:{e}")





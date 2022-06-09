import logging
import sys

from src.common.variable_files import COLL_OF_RAW_DATA, TWEET_KEY, ID_KEY, DATABASE_TWEET_NEW_DB
from src.encryption_and_decryption_data.encrypt_and_decryption_data import encrypt_string

LOGGER = logging.getLogger(__name__)


def insert_preprocessed_data(tweet_list,db):
    """
    store the raw data in the mongodb collection
    :param
    tweet_raw_data: initialize the mongodb collection
    """
    print("inside mongodb",tweet_list,len(tweet_list))
    updated_list =[]
    tweet_raw_data = db[COLL_OF_RAW_DATA]

    for message in tweet_list:
        try:
            if message:
                message[TWEET_KEY] = encrypt_string(message[TWEET_KEY])
                updated_list.append(message)
            else:
                LOGGER.info(f"Message:data is not in proper format {message}")
        except Exception as e:
            LOGGER.error(f"Error:{e}")
    print("\ndata is encrypted now\n")
    try:
        tweet_raw_data.insert_many(updated_list,ordered = False)
        print("Try check length of list", len(updated_list),updated_list)
    except Exception as e:
        remove_id = None
        for idx in range(len(e.details['writeErrors'])):
            for idx2 in range(len(tweet_list)):
               if tweet_list[idx2]['_id'] == e.details['writeErrors'][idx]['keyValue']['_id']:
                   remove_id=idx2
                   break
            del tweet_list[remove_id]
            remove_id=None
        print('bulk error is ',len(e.details['writeErrors']))


# message = [{"_id":'1534490445342035970',"tweet": "this is new tweet and present in mongodb"},{"_id":"hsdsdffasdssdfdffpsdfssdfdpysdmc singd","tweet":"unique tweet hai bhai"}]
# insert_preprocessed_data(message,db)





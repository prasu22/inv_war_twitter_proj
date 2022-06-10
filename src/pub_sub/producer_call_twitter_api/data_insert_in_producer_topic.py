from datetime import datetime
from time import sleep

import logging


from src.common.variable_files import TOPIC3

LOGGER = logging.getLogger(__name__)

def search_api_to_insert_data_in_topic(crawler_object,my_producer,keyword):
    tweets_data = crawler_object.fetch_tweets_from_search_api(keyword)
    for tweet in tweets_data:
        country = tweet._json['user']['location']
        if len(country) > 0:
            id = tweet._json['id']
            full_text = tweet._json['full_text']
            created_at = tweet._json['created_at']
            new_datetime = datetime.strptime(str(datetime.strptime(created_at, '%a %b %d %H:%M:%S +0000 %Y')),
                                             '%Y-%m-%d %H:%M:%S')
            try:
                my_data = {'_id': str(id), 'tweet': full_text, 'country': country, 'created_at': str(new_datetime)}
                print("search_apis",my_data)
                my_producer.send(TOPIC3, value=my_data)
                sleep(2)
            except Exception as e:
                LOGGER.error(e)
                pass

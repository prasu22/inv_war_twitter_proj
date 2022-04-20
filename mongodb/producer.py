#importing required libraries
from datetime import datetime
from json import dumps
from time import sleep
from kafka import KafkaProducer
from twiiter.twitter_api_connector import connect_with_twitter,collect_with_keyword

my_producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 11, 5),
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

# ======================================================================================================================
# generating the numbers ranging from 1 to 500
# code used when we use collect_with_keyword
api = connect_with_twitter()
keyword = "Vaccination And coronavirus -filter:retweets"
tweets_data = collect_with_keyword(api, keyword)
for tweet in tweets_data:
    country = tweet._json['user']['location']
    if len(country) > 0:
        id = tweet._json['id']
        status = api.get_status(id=id, tweet_mode="extended")
        full_text = status.full_text
        created_at = tweet._json['created_at']
        new_datetime = datetime.strptime(str(datetime.strptime(created_at, '%a %b %d %H:%M:%S +0000 %Y')),'%Y-%m-%d %H:%M:%S')
        try:
            my_data = {'_id': str(id),'tweet':full_text,'country':country,'created_at':str(new_datetime)}
            print(my_data)
            my_producer.send('sendingdata', value=my_data)
            sleep(2)
        except Exception as e:
            print(e)
            pass
# ======================================================================================================================

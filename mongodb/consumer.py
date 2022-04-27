import json
from kafka import KafkaConsumer
from mongodb.preprocessing_overall_tweet_data import preprocess_overalltweet, preprocess_total_tweet_per_country,preprocess_top_100_words, preprocess_top_10_precaustion_word, preprocess_total_number_of_donation, preprocess_tweets_based_on_trends


"""
 fetch the data from topic using consumer and  preprocess that data with for different collection before insertertion in collection
 :params
  my_consumer = store the all data from topic 
  message = access the data one by one from my_consumer
"""

my_consumer = KafkaConsumer(
        'sendingdata',
        bootstrap_servers=['localhost : 9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )


for message in my_consumer:
    message = message.value
    print(message['created_at'])
    try:
        if 'RT @' not in message['tweet']:
            # query1
            preprocess_overalltweet(message)
            # query2
            preprocess_total_tweet_per_country(message)
            # query3
            preprocess_top_100_words(message)
            #query5
            preprocess_top_10_precaustion_word(message)
            #query6
            preprocess_total_number_of_donation(message)
            #query8
            preprocess_tweets_based_on_trends(message)
    except Exception as e:
        print(f"error {e}")
        pass
# ======================================================================================================================
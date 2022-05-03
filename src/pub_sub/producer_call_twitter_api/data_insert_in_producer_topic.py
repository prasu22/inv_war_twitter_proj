from datetime import datetime
from time import sleep


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
                print("search", my_data)
                my_producer.send('sendingdata', value=my_data)
                sleep(2)
            except Exception as e:
                print(e)
                pass
def archive_api_to_insert_data_in_topic(crawler_object,my_producer,keyword):
    print("data stroed")
    tweets_details = crawler_object.fetch_tweets_from_30_days_api(keyword)
    for tweet in tweets_details:
        print(tweet)
        if (not tweet.retweeted) and ('RT @' not in tweet.text):
            if tweet.user.location != None:

                text = tweet["full_text"]
                # created_at = tweet.created_at.replace(tzinfo=None)
                new_dt = str(tweet.created_at)[:19]
                created_at = datetime.strptime(new_dt, '%Y-%m-%d %H:%M:%S')
                print(created_at)
                country = tweet.user.location
                try:
                    my_data = {'_id': str(id), 'tweet': text, 'country': country, 'created_at': str(created_at)}
                    print("new_data start:\n", my_data)
                    my_producer.send('sendingdata', value=my_data)
                    sleep(1)
                except Exception as e:
                    print(e)
                    pass
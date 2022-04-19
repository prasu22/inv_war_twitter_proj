import tweepy
from config import access_token,access_token_secret,consumer_key,consumer_secret

class StreamListener(tweepy.Stream):
    def on_status(selfself,status):
        if hasattr(status,'extended_tweet') and status.user.location != None:
            text = status.extended_tweet['full_text']
            print('text -----------> ',text)
            print('location--------->  ',status.user.location)
            print('create -------------> ',status.created_at)
            print('id--------> ',status.id)
            print('#####################'*10)


    def on_error(self,status_code):
        print("Encountered Streaming error (",status_code,")")
        if status_code == 420:
            return False



keywords=['wear mask']

def get_data_with_keyword(keywords):
    try:
        streamListener = StreamListener(consumer_key,consumer_secret,access_token,access_token_secret)
        streamListener.filter(track=keywords,languages=['en'])
        # for getting the tweets of specific user "follow = user_ids"
    except Exception as e:
        print(e)

get_data_with_keyword(keywords)

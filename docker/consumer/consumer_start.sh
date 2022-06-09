#!/usr/bin/env bash

python src/pub_sub/twitter_rest_api_consumer.py &
python src/pub_sub/tweepy_stream_consumer.py &
python src/pub_sub/tweepy_search_api_consumer.py
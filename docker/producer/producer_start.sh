#!/usr/bin/env bash

python src/twitter/fetch_data_using_twitter_rest_api.py &
python src/apps/keyword_producer.py

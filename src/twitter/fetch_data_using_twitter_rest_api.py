import requests
import os
import json
import logging

from src.twitter.config import bearer_token

LOGGER = logging.getLogger(__name__)

BEARER_TOKEN = bearer_token


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {BEARER_TOKEN}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    LOGGER.info(json.dumps(response.json()))
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    LOGGER.info(json.dumps(response.json()))


def set_rules(delete):
    # You can adjust the rules if needed
    sample_rules = [
        {
            "value": "(sanitiser OR stay donation OR Covid OR mask OR stock market OR stay home OR World health organization) lang:en -birthday -is:retweet",
            "tag": "#Covid #coronavirus #covid-19 #WHO #donation #mask"
        }

    ]
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))


def get_stream(set):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream?tweet.fields=created_at&expansions=author_id&user.fields=location", auth=bearer_oauth, stream=True,
    )
    # print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    import src.pub_sub.producer as prod
    for response_line in response.iter_lines():
        if response_line:
            # print(response_line)
            json_response = json.loads(response_line)
            print(type(json_response))
            value = json_response
            print(value)
            if value['includes']['users'][0].get("location") and  len(value['data']['text'])>0:
                my_data = {'_id': str(value['data']['id']), 'tweet': value['data']['text'], 'country': value['includes']['users'][0]['location'], 'created_at': str(value['data']['created_at'])}
                print("stream mydata", my_data)
                prod.my_producer.send('random_data', value=my_data)
            else:
                LOGGER.INFO("location is not available")



def main():
    rules = get_rules()
    delete = delete_all_rules(rules)
    set = set_rules(delete)
    get_stream(set)


if __name__ == "__main__":
    while True:
        try:
            main()
        except requests.exceptions.ChunkedEncodingError:
            LOGGER.info('restarting')
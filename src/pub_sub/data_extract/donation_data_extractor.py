import logging
from src.common.app_config import APP_CONFIG

LOGGER = logging.getLogger(__name__)
import re
from src.common.variable_files import DEFAULT_AMOUNT, CURRENCY_MAPPING, DEFAULT_CURRENCY_NAME, \
    DONATION_AMOUNT_KEY, TWEET_KEY, CURRENCY_NAME_KEY, DONATION_KEYWORDS_KEY




DONATION_KEYWORDS = list(map(str, APP_CONFIG.getlist('keywords', 'DONATION_KEYWORDS')))


def get_donation_amount(message):
    try:
        if re.compile('[$¢£¤¥֏؋৲৳৻૱௹฿៛₹](\s?)(\d[ 0-9,.]+)(k)?(m)?(b)?(M)?(B)?(cr)?(Cr)?').search(message[TWEET_KEY]):
            amount = re.compile('[$¢£¤¥֏؋৲৳৻૱௹฿៛₹](\s?)(\d[ 0-9,.]+)(k)?(m)?(b)?(M)?(B)?(cr)?(Cr)?').search(message[TWEET_KEY]).group(0)
            value = float(re.sub(r'[^\d.]', '', amount))
            message[DONATION_AMOUNT_KEY] = value
            return message

        elif re.compile(r"(\d[ 0-9,.]+)(k)?(m)?(b)?(M)?(B)?(cr)?(Cr)?(\s?)(USD\b|\bINR\b)", re.IGNORECASE).search(message[TWEET_KEY]):
            amount = re.compile(r"(\d[ 0-9,.]+)(k)?(m)?(b)?(M)?(B)?(cr)?(Cr)?(\s?)(USD\b|\bINR\b)", re.IGNORECASE).search(message[TWEET_KEY]).group(0)
            value = float(re.sub(r'[^\d.]', '', amount))
            message[DONATION_AMOUNT_KEY] = value
            return message

        else:
            message[DONATION_AMOUNT_KEY] = DEFAULT_AMOUNT
            return message
    except Exception as e:
        LOGGER.error(f"ERROR:{e} ")


def get_donation_currency(message):
    try:
        if re.compile('[$¢£¥฿€₹](\s?)(\d[ 0-9,.]+)').search(message[TWEET_KEY]):
            currency_symbol = re.compile('[$¢£¥฿€₹]').search(message[TWEET_KEY]).group(0)
            message[CURRENCY_NAME_KEY] = CURRENCY_MAPPING[currency_symbol]
            return message

        elif re.compile(r"(\d[ 0-9,.]+)(k)?(m)?(b)?(M)?(B)?(cr)?(Cr)?(\s?)(USD\b|\bINR\b)", re.IGNORECASE).search(
                message[TWEET_KEY]):
            currency_name = re.compile(r"\bUSD\b|\bINR\b", re.IGNORECASE).search(message[TWEET_KEY]).group(0)
            message[CURRENCY_NAME_KEY] = currency_name.upper()
            return message

        else:
            message[CURRENCY_NAME_KEY] = DEFAULT_CURRENCY_NAME
            return message
    except Exception as e:
        LOGGER.error(f"ERROR:{e} ")


def get_donation_keywords(message):
    list_of_donation_keywords = []
    try:
        if re.compile('|'.join(DONATION_KEYWORDS), re.IGNORECASE).search(message[TWEET_KEY]):
            list_of_donation_keywords = re.compile('|'.join(DONATION_KEYWORDS), re.IGNORECASE).findall(message[TWEET_KEY])
        else:
            LOGGER.info(f"Data Not found in tweet! ")
        message[DONATION_KEYWORDS_KEY] = list_of_donation_keywords
        return message
    except Exception as e:
        LOGGER.error(f"ERROR:{e} ")







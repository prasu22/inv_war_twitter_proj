import logging
LOGGER = logging.getLogger(__name__)
import re

from src.common.variable_files import COUNTRY_CODE_MAPPED, COUNTRY_NAME, COUNTRY_CODE, COUNTRY_NAME_KEY,COUNTRY_CODE_KEY



def get_country_code(message):
    list_of_country = list(COUNTRY_CODE_MAPPED.keys())
    country = re.compile('|'.join(list_of_country), re.IGNORECASE).search(message[COUNTRY_NAME_KEY])
    try:

        if country:
            country = re.compile('|'.join(list_of_country), re.IGNORECASE).search(message[COUNTRY_NAME_KEY]).group(0).title()
            country_code = COUNTRY_CODE_MAPPED[country]
            message[COUNTRY_NAME_KEY] = country
            message[COUNTRY_CODE_KEY] = country_code
            return message
        else:
            message[COUNTRY_NAME_KEY] = COUNTRY_NAME
            message[COUNTRY_CODE_KEY] = COUNTRY_CODE
            return message
    except Exception as e:
        LOGGER.error(f"ERROR:{e}")

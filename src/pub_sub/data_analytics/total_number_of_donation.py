import logging

LOGGER = logging.getLogger(__name__)
from src.common.variable_files import COLL_OF_DONATION_PER_COUNTRY
from src.common.variable_files import COUNTRY_NAME_KEY, COUNTRY_CODE_KEY, DONATION_AMOUNT_KEY, CURRENCY_NAME_KEY, \
   DONATION_KEYWORDS_KEY, COVID_KEYWORD_KEY, COUNT_KEY




def analysis_of_total_number_of_donation(message, output_dictionary, db):
   """
          store the data in collection after manupulation in mongodb collection count_of_donation_in_covid
          :collection schema
            {
              _id: objectid
              country:string
              country_code: string
              count: int
              date: string
           }
          :passing argument
          message : dictionary storing information of tweet
          db : store the database
          :param
          country_name = store the country name
          country_code = store the country code
          donation_data = store the list of keyword present in tweet related to donation
          currency_name = store the currency name in short form
          amount = store the donation amount present in  the tweet
          covide_data = store the list of covid keyword present in tweet

   """
   try:
       amount = message[DONATION_AMOUNT_KEY]
       covid_data = message[COVID_KEYWORD_KEY]
       donation_data = message[DONATION_KEYWORDS_KEY]
       currency_name = message[CURRENCY_NAME_KEY]
       country_code = message[COUNTRY_CODE_KEY]
       country_name = message[COUNTRY_NAME_KEY]

       if len(covid_data) > 0 and len(donation_data) > 0 and currency_name != "NO_Currency" and amount > 0:
           print(country_name, country_code, amount, currency_name)

           if db[COLL_OF_DONATION_PER_COUNTRY].count_documents({COUNTRY_NAME_KEY: country_name, CURRENCY_NAME_KEY: currency_name}) == 0:
               db[COLL_OF_DONATION_PER_COUNTRY].insert_one(
                   {COUNT_KEY: 1, COUNTRY_NAME_KEY: country_name, COUNTRY_CODE_KEY: country_code,
                    DONATION_AMOUNT_KEY: amount,
                    CURRENCY_NAME_KEY: currency_name})

               # for validation
               country_code = country_code
               if country_code not in output_dictionary:
                   output_dictionary[country_code] = 1
               else:
                   output_dictionary[country_code] += 1
           else:
               db[COLL_OF_DONATION_PER_COUNTRY].update_one({COUNTRY_NAME_KEY: country_name, CURRENCY_NAME_KEY: currency_name},
                                                           {'$inc': {COUNT_KEY: 1, DONATION_AMOUNT_KEY: amount}})

               # for validation
               country_code = country_code
               if country_code not in output_dictionary:
                   output_dictionary[country_code] = 1
               else:
                   output_dictionary[country_code] += 1
       else:
           LOGGER.info(f"MESSAGE:Data is not found! ")
   except Exception as e:
       LOGGER.error(f"ERROR:{e} ")


def updated_donation_list_total_tweets(tweet_list, db):
   output_dictionary = {}
   for message in tweet_list:
       analysis_of_total_number_of_donation(message, output_dictionary, db)
   return output_dictionary




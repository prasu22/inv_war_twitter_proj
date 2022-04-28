from pub_sub.data_extraction.extract_country_code import get_country_code
from pub_sub.data_extraction.extract_donation_amount import get_donation_amount
from pub_sub.data_extraction.extract_donation_currency import get_donation_currency
from pub_sub.data_extraction.extract_tweets_by_keywords import get_tweets_with_keyword

keywords = ["covid","corona","coronavirus"]
donation_keyword = ["donation","Money","contribution","donate","contribute","fund"]
def analysis_of_total_number_of_donation(message,db):
        amount = get_donation_amount(message)
        print(amount ,type(amount))
        covid_data =  get_tweets_with_keyword(message,keywords)
        donation_data = get_tweets_with_keyword(message,donation_keyword)
        currency = get_donation_currency(message)
        country_data = get_country_code(message)

        if amount and currency and country_data and covid_data and donation_data:
            country_code = country_data['country_code']
            country = country_data['country']
            print(country,country_code,amount,currency)

            if db['a_total_number_of_donation_per_country'].count_documents({"country": country}) == 0:
                db['a_total_number_of_donation_per_country'].insert_one({'count': 1, 'country': country,'country_code': country_code,"amount":amount,"currency":currency})
            else:
                db['a_total_number_of_donation_per_country'].update_one({"country": country},{'$inc': {'count': 1,"amount":amount}})
            print("donation part inserted data success fullly")
            return True
        else:
            return False
# analysis_of_total_number_of_donation({"tweet":"people donate in covid for poor people or affected people $3234","country":"India"})


def analysis_of_total_number_of_donation(message,db):
        amount = message['donation_amount']
        print(amount, type(amount))
        covid_data =  message['covid_keywords']
        donation_data = message['donation_keywords']
        currency_name = message['currency_name']
        country_code= message['country_code']
        country_name = message['country']

        if len(covid_data) > 0 and len(donation_data) > 0 and currency_name != "NO_Currency" and amount > 0:
            print(country_name,country_code,amount,currency_name)

            if db['a_total_number_of_donation_per_country'].count_documents({"country": country_name}) == 0:
                db['a_total_number_of_donation_per_country'].insert_one({'count': 1, 'country': country_name,'country_code': country_code,"amount":amount,"currency":currency_name})
            else:
                db['a_total_number_of_donation_per_country'].update_one({"country": country_name},{'$inc': {'count': 1,"amount":amount}})
            print('i am analysis_of_total_number_of_donation')
# analysis_of_total_number_of_donation({"tweet":"people donate in covid for poor people or affected people $3234","country":"India"})
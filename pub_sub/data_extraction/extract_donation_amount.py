# get amount of donation
import re


def get_donation_amount(message):

   if re.compile('[$¢£¤¥֏؋৲৳৻૱௹฿៛₹](\s?)(\d[ 0-9,.]+)').search(message['tweet']):
       amount = re.compile('[$¢£¤¥֏؋৲৳৻૱௹฿៛₹](\s?)(\d[ 0-9,.]+)').search(message['tweet']).group(0)
       value = float(re.sub(r'[^\d.]', '', amount))
       return value
   elif re.compile(r"(\d[ 0-9,.]+)(\s?)(\bUSD\b|\bINR\b)",re.IGNORECASE).search(message['tweet']):
       amount = re.compile(r"(\d[ 0-9,.]+)(\s?)(\bUSD\b|\bINR\b)",re.IGNORECASE).search(message['tweet']).group(0)
       value = float(re.sub(r'[^\d.]', '', amount))
       return value
   else:
       return None



# print(get_donation_amount({'tweet':'ahhelsd donatipn 23   kjsf  $ 2374.6 '}))
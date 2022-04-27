# get amount of donation
import re


def get_donation_amount(message):

   if re.compile('[$¢£¤¥֏؋৲৳৻૱௹฿៛₹](\d[ 0-9,.]+)').search(message['tweet']) or re.compile('[$¢£¤¥֏؋৲৳৻૱௹฿៛₹]').search(message['tweet']):
       amount = re.compile('(\d[0-9,.]+)').search(message['tweet']).group(0)
       value = int(re.sub(r'[^\d.]', '', amount))
       return value
   elif re.compile(r"\bUSD\b|\bINR\b",re.IGNORECASE).search(message['tweet']) and re.compile(r"(\d[ 0-9,.]+)").search(message['tweet']) :
       amount = re.compile(r"(\d[ 0-9,.]+)").search(message['tweet']).group(0)
       value = int(re.sub(r'[^\d.]', '', amount))
       return value
   else:
       return None



print(get_donation_amount({'tweet':'ahhelsd donatipn $ 2374 USD'}))
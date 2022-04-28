import re

currency_symbols ={"$":'USD',"¢":"GHS","£":"EGP","¥":"JPY","฿":"THB","€":"EUR","₹":"INR"}

def get_donation_currency(message):

   if re.compile('[$¢£¥฿€₹](\s?)(\d[ 0-9,.]+)').search(message['tweet']):
       symbol = re.compile('[$¢£¥฿€₹]').search(message['tweet']).group(0)
       return currency_symbols[symbol]
   elif re.compile(r"(\d[ 0-9,.]+)(k)?(m)?(b)?(M)?(B)?(cr)?(Cr)?(\s?)(USD\b|\bINR\b)",re.IGNORECASE).search(message['tweet']) :
       symbol = re.compile(r"\bUSD\b|\bINR\b",re.IGNORECASE).search(message['tweet']).group(0)
       return symbol.upper()
   else:
       return None



# print(get_donation_currency({"tweet":"this is new donation  234 cr INR  "}))
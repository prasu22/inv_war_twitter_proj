# get amount of donation
import re

CURRENCY_MAPPING ={"$":'USD',"¢":"GHS","£":"EGP","¥":"JPY","฿":"THB","€":"EUR","₹":"INR"}
DONATION_KEYWORDS = ["donation","Money","contribution","donate","contribute","fund"]
DEFAULT_CURRENCY_NAME = "NO_Currency"
DEFAULT_AMOUNT = 0
def get_donation_amount(message):
   if re.compile('[$¢£¤¥֏؋৲৳৻૱௹฿៛₹](\s?)(\d[ 0-9,.]+)').search(message['tweet']):
       amount = re.compile('[$¢£¤¥֏؋৲৳৻૱௹฿៛₹](\s?)(\d[ 0-9,.]+)').search(message['tweet']).group(0)
       value = float(re.sub(r'[^\d.]', '', amount))
       message['donation_amount'] = value
       return message
   elif re.compile(r"(\d[ 0-9,.]+)(\s?)(\bUSD\b|\bINR\b)",re.IGNORECASE).search(message['tweet']):
       amount = re.compile(r"(\d[ 0-9,.]+)(\s?)(\bUSD\b|\bINR\b)",re.IGNORECASE).search(message['tweet']).group(0)
       value = float(re.sub(r'[^\d.]', '', amount))
       message['donation_amount'] = value
       return message
   else:
       message['donation_amount'] = DEFAULT_AMOUNT
       return message



def get_donation_currency(message):
   if re.compile('[$¢£¥฿€₹](\s?)(\d[ 0-9,.]+)').search(message['tweet']):
       currency_symbol = re.compile('[$¢£¥฿€₹]').search(message['tweet']).group(0)
       message['currency_name']=CURRENCY_MAPPING[currency_symbol]
       return message

   elif re.compile(r"(\d[ 0-9,.]+)(k)?(m)?(b)?(M)?(B)?(cr)?(Cr)?(\s?)(USD\b|\bINR\b)",re.IGNORECASE).search(message['tweet']) :
       currency_name = re.compile(r"\bUSD\b|\bINR\b",re.IGNORECASE).search(message['tweet']).group(0)
       message['currency_name'] = currency_name.upper()
       return message
   else:
       message['currency_name']=DEFAULT_CURRENCY_NAME
       return message

def get_donation_keywords(message):
    list_of_donation_keywords = []
    if re.compile('|'.join(DONATION_KEYWORDS), re.IGNORECASE).search(message['tweet']):
        list_of_donation_keywords = re.compile('|'.join(DONATION_KEYWORDS), re.IGNORECASE).findall(message['tweet'])
    message["donation_keywords"] = list_of_donation_keywords
    return message

# ABC = get_donation_currency({"tweet":"this is new donation  234 cr INR  "})
#
# print(ABC)
#
# print(get_donation_amount(ABC))
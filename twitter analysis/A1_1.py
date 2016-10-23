import json
import urllib
import requests
from requests_oauthlib import OAuth1
from TwitterAPI import TwitterAPI, TwitterRestPager

#SEARCH_TERM = 'microsoft&page=2'
ACCESS_TOKEN = '761101322527449088-drPGxygZvdAGsvOQuxEf9E1wvzdXJid'
ACCESS_SECRET = 'tBKckOB9F2xFF4IPp4DmThjQwqg9I6jxgexslZNSqVSRh'
CONSUMER_KEY = 'rtUDXzjPP6efmUOksEPz24Kqz'
CONSUMER_SECRET = '0IgI64LKGwKNEjb4rd88o9kyu4EhCmedKw4M5tbDRrC9rBKumU'

auth = TwitterAPI(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)

r = auth.request('search/tweets',{'q':"microsoft&page=1"})
for item in r:
    print item['text']
print '----------------------------------'



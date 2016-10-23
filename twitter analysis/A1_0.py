import urllib
import requests
from requests_oauthlib import OAuth1
from TwitterAPI import TwitterAPI, TwitterRestPager
import json

#SEARCH_TERM = 'microsoft&page=2'
ACCESS_TOKEN = '761101322527449088-drPGxygZvdAGsvOQuxEf9E1wvzdXJid'
ACCESS_SECRET = 'tBKckOB9F2xFF4IPp4DmThjQwqg9I6jxgexslZNSqVSRh'
CONSUMER_KEY = 'rtUDXzjPP6efmUOksEPz24Kqz'
CONSUMER_SECRET = '0IgI64LKGwKNEjb4rd88o9kyu4EhCmedKw4M5tbDRrC9rBKumU'

#url = "https://api.twitter.com/1.1/search/tweets.json?q=microsoft"
auth = TwitterAPI(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)
#res = TwitterAPI(auth=auth)
r = auth.request('search/tweets',{'q':"microsoft&page=1"})
for item in r:
    print item['text']
print '----------------------------------'
r = auth.request('search/tweets',{'q':"microsoft&page=2"})
for item in r:
    print item['text']
print '----------------------------------'
#api = TwitterAPI(CONSUMER_KEY,
                 #CONSUMER_SECRET,
                 #ACCESS_TOKEN,
                 #ACCESS_SECRET)
#print data
#pager = TwitterRestPager(api, 'search/tweets', {'q': SEARCH_TERM})


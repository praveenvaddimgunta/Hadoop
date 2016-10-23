import urllib
import requests
from requests_oauthlib import OAuth1
from TwitterAPI import TwitterAPI, TwitterRestPager
import json
 
consumer_key = 'IyxWZqvSOFd9qKNo9eyzjcX0z'
consumer_secret = 'EB6m9y7EepIWS8EXcfw4Qw8jPxTiu4JXoFA5im7jshGKUozkxY'
access_token = '777438044194418692-XqUy0IZSPCyoBRV9KHVPNt79wOvwUw5'
access_secret = '9NI8rBfKtpclvzTqb4VxFJZ5ZQRfc0fdl7YPE20vqpH2c'
 
url = "https://api.twitter.com/1.1/search/tweets.json?q=microsoft"
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


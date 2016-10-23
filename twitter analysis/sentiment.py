import json
import re

#
# Read sentiment file and put into dictionary.
#
afinnfile = open("AFINN-111.txt","r")
scores = {}
term = ""
score = 0
list_lines = []
lines = afinnfile.readlines()

for i in lines:
    term, score = i.split("\t")
    scores[term] = int(score)


with open("1000_test_tweets.json") as f:
    l = f.readlines()
    for i in range(len(l)):
        s = 0
        res = json.loads(l[i])
        try:
            t = res["text"].encode("utf-8")
            words = re.findall("[^\s]+",t)
            for j in words:
                if j.lower() in scores:
                    s = s + scores[j]
                    print j
                print s
        except KeyError:
            print s



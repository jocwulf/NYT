import urllib.parse
import requests
import json
from pymongo import MongoClient
import datetime
import time
import re

#####Environment Vars
#add API key
NYTKEY = ""

def get_comment(url,offset):
    '''issue individual api call'''
    apiaddress = 'https://api.nytimes.com/svc/community/v3/user-content/url.json?api-key='
    fullquery = apiaddress+ NYTKEY + '&url=' + str(url) + '&offset=' + str(offset)
    # sleep 6 seconds in order not to hit quota
    time.sleep(6)
    print("query: {}".format(fullquery))
    session = requests.Session()
    try:
        response = session.get(fullquery)
        response_json = json.loads(response.text)
    except Exception as e:
        try:
            response = session.get(fullquery)
            response_json = json.loads(response.text)
        except Exception as e:
            print("ERROR: skipping page" + page + " due to error: " + str(e))
            session.close()
            return None

    session.close()
    return response_json

def write_to_mongo(collector_json,article_id,db):
    '''writes comment information to article with respective ID'''
    db.nyt_articles.update_one({"_id": article_id}, {"$set": {"comments": collector_json}}, upsert=False)
    return

def write_comment(comment_json,collector_json,article_id,offset,db):
    '''if we have all comments, write it into mongodb, otherwise append to collector and update offest info'''
    # extend comments in collector
    if offset > 0:  # collector not equal to comment_json
        #collector_json["results"]["comments"] = collector_json["results"]["comments"].extend(comment_json["results"]["comments"])  # extend comments in collector
        list1 = list(collector_json["results"]["comments"] )
        list2 = list(comment_json["results"]["comments"] )
        list3 = list1+list2
        collector_json["results"]["comments"] = list3
    totalParentCommentsFound=int(comment_json["results"]["totalParentCommentsFound"])
    if (totalParentCommentsFound-offset)>25: #there is still data left
        offset += 25
        return (offset, collector_json,"go on")
    else:
        #write to mongo
        write_to_mongo(collector_json,article_id,db)
        return (offset, collector_json,"done")

def get_comments(url_encoded,article_id,db):
    '''save all comments relating to a single article'''
    offset=0 #initial value
    status = "go on" #initial value
    while status == "go on":
        comment_json = get_comment(url_encoded,offset)
        if "results" in comment_json and 'totalParentCommentsReturned' in comment_json['results']: #we can work with the results
            if offset == 0:
                collector_json=comment_json
            (offset, collector_json, status) = write_comment(comment_json, collector_json, article_id, offset,db)
        else: #results not usable
            print("non usable message for article: {}".format(url_encoded))
            print(str(comment_json))
            status = "done"

    #combine potentially multiple comment pages smartly
    return

def insert_comments():
    '''insert comments for nyt articles in DB'''

    #open db connection#open mongodb connection
    client = MongoClient('localhost:27017')
    db = client.nyt_db

    #search for NYT articles since 2007 with autonomous vehicle keywords
    ##regx is a little cumbersome...
    regx1 = re.compile("driverless car", re.IGNORECASE)
    regx2 = re.compile("robotic car", re.IGNORECASE)
    regx3 = re.compile("autonomous car", re.IGNORECASE)
    regx4 = re.compile("self-driving car", re.IGNORECASE)
    regx5 = re.compile("vehicle", re.IGNORECASE)
    regx6 = re.compile("automotive", re.IGNORECASE)
    regx7 = re.compile("automobile", re.IGNORECASE)
    regx8 = re.compile("autonomous driving", re.IGNORECASE)

    regx9 = re.compile("driverless", re.IGNORECASE)
    regx10 = re.compile("self-driving", re.IGNORECASE)
    regx11 = re.compile("robotic", re.IGNORECASE)
    regx12 = re.compile("autonomous", re.IGNORECASE)

    #loop over articles of interest
    for article in db.nyt_articles.find({"$and": [{"comments": {"$exists": 0}}, {"source": "The New York Times"}, {
        "$or": [{"keywords.value": "Driverless and Semiautonomous Vehicles"}, {
            "$and": [{"abstract": {"$in": [regx1, regx2, regx3, regx4, regx5, regx6, regx7, regx8]}},
                     {"abstract": {"$in": [regx9, regx10, regx11, regx12]}}]}]}]}):
    #iterate through articles
        #we are only interested in articles with potential comments, so type must be nyt and year 2007 upwards
        timestamp = article["pub_date"]
        date_time_obj = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S+%f')
        if date_time_obj.year > 2006 and article["source"]=="The New York Times":
            #encode url
            url_encoded = urllib.parse.quote_plus(article["web_url"])
            article_id = article["_id"]
            print("start work on article: {}".format(url_encoded))
            get_comments(url_encoded,article_id,db)
    return

##########define main function that orchestrates process
def main():
    insert_comments()
    print("done")

if __name__ == '__main__':
    main()


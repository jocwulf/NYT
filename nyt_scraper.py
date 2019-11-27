import requests
import json
from pymongo import MongoClient
import datetime
import time

#####Environment Vars
#add API key
NYTKEY = ""

def perform_query(searchterm,begindate,page):
    '''downloads a single page from NYT
    f(string,string,string)-> json'''
    apiaddress = 'https://api.nytimes.com/svc/search/v2/articlesearch.json?q='
    fullquery = apiaddress+searchterm+'&api-key='+NYTKEY+'&page='+str(page)+'&sort=oldest&begin_date='+begindate
    #sleep 6 seconds in order not to hit quota
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

def write_mongodb(article_json,db):
    '''writes single entry into MonoDB'''
    #check whether article already in db
    article_id = article_json['_id']
    if not db.nyt_articles.find_one({"_id":article_id}):
        db.nyt_articles.insert_one(article_json)
    return

def get_date_plus_one(lastdate):
    '''add one day and return the date string
    f(str)->str'''
    date_time_obj = datetime.datetime.strptime(lastdate, '%Y-%m-%dT%H:%M:%S+%f')
    date_plus_one = date_time_obj + datetime.timedelta(days=1)
    return date_plus_one.strftime('%Y%m%d')

def write_articles(response_json,page,begindate,db):
    '''iterates over articles in single page and writes to mongodb, updates page and begindate
    f(json,int,str,db)-> str,str'''
    if "status" in response_json and response_json["status"]=="OK":

        #iterate over the responses
        #write into MongoDB while remembering the last date
        for article_json in response_json['response']['docs']:
            last_date = article_json["pub_date"]
            write_mongodb(article_json, db)

        #check how to issue next query to nyt api
        offset = int(response_json['response']['meta']['offset'])
        hits = int(response_json['response']['meta']['hits'])
        if ((hits - offset) > 10): #there is more to download
            if page < 99:
                page += 1 #we can retrieve the next page
            else:
                begindate = get_date_plus_one(last_date)
                page = 0 #me must reconfigure the query
            return (begindate,str(page))
        else:
            return (begindate,"done") #signal that we have retrieved all
    else:
        print("error with json answer, status is not ok, we will abort")
        print("response: {}".format(response_json))
        return (begindate, "done")

def get_articles(searchterm):
    '''initiates queries until all data is downloaded
    string -> '''
    #set initial page and begindate values
    page="0"
    #page='28'
    #begindate='19800101'
    begindate='20070101'


    #open mongodb connection
    client = MongoClient('localhost:27017')
    db = client.nyt_db

    while page != "done": #there is more to download
        response_json = perform_query(searchterm,begindate,page)
        begindate, page = write_articles(response_json, int(page), begindate, db)

    print("done with downloading search string {}".format(searchterm))

    #close mongdb connection
    client.close()
    return



##########define main function that orchestrates process
def main():
    '''autonomous+car: 2385
autonomous+vehicle: 226
driverless+vehicle: 691
driverless+car: 1076
robotic+car: 4305
robotic+vehicle: 2314
self-driving+car: 89617
self-driving+vehicle: 31833
'''
    #searchterm='autonomous+car'
    #searchterm = 'self-driving+car'
    #termlist = ('autonomous+vehicle','driverless+vehicle','driverless+car','robotic+car','robotic+vehicle','selfdriving+vehicle')

    #searchterm = 'robotic+vehicle'
    #searchterm = 'self-driving+vehicle'
    #searchterm= 'autonomous+driving'
    searchterm='robotic+car' #page=54 begindate=20101106
    get_articles(searchterm)
    print("all done")

if __name__ == '__main__':
    main()
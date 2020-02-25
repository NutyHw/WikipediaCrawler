import json
import requests
import pymongo
import random
import copy
import logging

logging.basicConfig(filename='main.log',
                    format='%(asctime)s - %(process)d - %(levelname)s - %(message)s',
                    level=logging.DEBUG
                    )

def proxy():
    allProxy = list()
    with open('proxy.txt') as f:
        allProxy = json.load(f)
    return allProxy

allProxy = proxy()

def wikiCrawler(query):
    '''
    return all result of requests from wiki api
    
    parameter :
        query : list of json query parameter 
    '''
    
    global allProxy

    api = 'https://th.wikipedia.org/w/api.php'

    while True:
        chosenProxy = random.choice(allProxy)
        try:
            r = requests.get(api , params = query , proxies = chosenProxy, timeout = 10).json()

            if not 'query' in r.keys():
                break

            yield r['query']['pages']

            if 'continue' in r.keys():
                query.update(r['continue'])
            else:
                break

        except requests.exceptions.RequestException:
            continue
        except Exception:
            break

def pipeline(res, collection):
    client = pymongo.MongoClient('localhost:27017',
                                username = 'nuty',
                                password = 'Oraphin123456789',
                                authSource = 'wikipedia', 
                                authMechanism = 'SCRAM-SHA-1'
                                )
    db = client['wikipedia'][collection]
    db.insert_many(res)

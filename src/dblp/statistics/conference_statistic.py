import pymongo
from tqdm import tqdm
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import math

def processConference():
    mongoClient = pymongo.MongoClient('mongodb://172.29.7.234:27017/')

    db = mongoClient.admin
    db.authenticate('mongo', 'zrgwmx', mechanism='SCRAM-SHA-1')

    mongoDb = mongoClient['paper']
    mongoColl = mongoDb['dblp2']
    mongoColl2 = mongoDb['dblp_venues']
    mongoColl4 = mongoDb['dblp_conferences']

    for item in tqdm(mongoColl2.find({})):
        paperCount = 0
        citationCount = 0
        heat = 0
        years = {}
        if 'papers' in item.keys():
            paperCount = len(item['papers'])
            for paperId in item['papers']:
                paper = mongoColl.find_one({'_id': paperId})
                if 'n_citation' in paper.keys():
                    citationCount = citationCount + paper['n_citation']
                if 'year' in paper.keys():
                    year = str(paper['year'])
                    if year in years.keys():
                        years[year] = years[year] + 1
                    else:
                        years[year] = 1
        sum = 0
        for syear in years.keys():
            year = int(syear)
            sum = sum + math.ceil(years[syear] / ((2022 - year) / 3))
        heat = sum
        item.update({
            'paperCount': paperCount,
            'citationCount': citationCount,
            'years': years,
            'heat': heat
        })

        mongoColl4.insert_one(item)



def toEs():
    mongoClient = pymongo.MongoClient('mongodb://172.29.7.234:27017/')

    db = mongoClient.admin
    db.authenticate('mongo', 'zrgwmx', mechanism='SCRAM-SHA-1')

    mongoDb = mongoClient['paper']

    mongoColl2 = mongoDb['dblp_conferences']

    es = Elasticsearch(['localhost:9200'])
    actions = []
    for item in tqdm(mongoColl2.find()):
        id=item['_id']
        item.pop('_id')
        action = {
            "_index": "dblp_conferences",
            "_id": id,
            "_source": item
        }
        actions.append(action)
        if len(actions) == 1000:
            helpers.bulk(es, actions)
            del actions[0:len(actions)]


    helpers.bulk(es, actions)
def deleteEs():
    es = Elasticsearch(['localhost:9200'])
    es.indices.delete('dblp_conferences')

if __name__ == '__main__':
    # processConference()
    # deleteEs()
    toEs()
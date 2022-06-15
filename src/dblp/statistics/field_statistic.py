import pymongo
import math
from tqdm import tqdm
from elasticsearch import Elasticsearch
from elasticsearch import helpers


# 热度=求和(每一年的论文数/((2022-那一年)/3))


def processField():
    mongoClient = pymongo.MongoClient('mongodb://172.29.7.234:27017/')

    db = mongoClient.admin
    db.authenticate('mongo', 'zrgwmx', mechanism='SCRAM-SHA-1')

    mongoDb = mongoClient['paper']

    mongoColl2 = mongoDb['dblp2']

    mongoColl3 = mongoDb['dblp_fields']
    mongoColl4 = mongoDb['dblp_fields_2']
    index = 0
    for item in tqdm(mongoColl3.find({})):
        paperCount = 0
        citationCount = 0
        heat = 0
        years = {}
        if 'paper' in item.keys():
            paperCount = len(item['paper'])
            for paper_ in item['paper']:
                paperId = paper_['id']
                paper = mongoColl2.find_one({'_id': paperId})
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
        name = item['_id']
        item.pop('_id')
        item.update({
            '_id': index,
            'name': name,
            'paperCount': paperCount,
            'citationCount': citationCount,
            'years': years,
            'heat': heat
        })
        index = index + 1
        mongoColl4.insert_one(item)


def processField_2():
    mongoClient = pymongo.MongoClient('mongodb://172.29.7.234:27017/')

    db = mongoClient.admin
    db.authenticate('mongo', 'zrgwmx', mechanism='SCRAM-SHA-1')

    mongoDb = mongoClient['paper']

    mongoColl2 = mongoDb['dblp_fields_2']
    mongoColl3 = mongoDb['dblp_fields']

    for item in tqdm(mongoColl2.find()):
        papers = []
        if 'paper' in item.keys():
            papers_temp = item['paper']
            for paper in papers_temp:
                papers.append(paper['id'])
            item.pop('paper')
            item.update({
                'papers': papers
            })
        mongoColl3.insert_one(item)


def processField_3():
    mongoClient = pymongo.MongoClient('mongodb://172.29.7.234:27017/')

    db = mongoClient.admin
    db.authenticate('mongo', 'zrgwmx', mechanism='SCRAM-SHA-1')

    mongoDb = mongoClient['paper']
    mongoColl = mongoDb['dblp2']
    mongoColl3 = mongoDb['dblp_fields']
    mongoColl2 = mongoDb['dblp_fields_2']

    for item in tqdm(mongoColl2.find()):

        if 'papers' in item.keys():
            papers = item['papers']
            if len(papers) > 150:
                papers=papers[:150]

            item.pop('papers')
            item.update({
                'papers': papers
            })
        mongoColl3.insert_one(item)

def processField_4():
    mongoClient = pymongo.MongoClient('mongodb://172.29.7.234:27017/')

    db = mongoClient.admin
    db.authenticate('mongo', 'zrgwmx', mechanism='SCRAM-SHA-1')

    mongoDb = mongoClient['paper']
    mongoColl = mongoDb['dblp2']
    mongoColl2 = mongoDb['dblp_fields']
    mongoColl3 = mongoDb['dblp_fields_3']

    for item in tqdm(mongoColl2.find()):

        if 'papers' in item.keys():
            papers = item['papers']
            authorIds={}
            for paperId in papers:
                paper=mongoColl.find_one({'_id': paperId})
                if 'authors' in paper.keys():
                    for author in paper['authors']:
                        if author['id'] in authorIds:
                            authorIds[author['id']]=authorIds[author['id']]+1
                        else:
                            authorIds[author['id']] = 1
            authorIds = {k: v for k, v in sorted(authorIds.items(), key=lambda item: item[1], reverse=True)}
            temp=[]
            for key in authorIds.keys():
                temp.append(key)
                if len(temp)>=10:
                    break
            item.update({
                'authors': temp
            })

        mongoColl3.insert_one(item)


def toEs():
    mongoClient = pymongo.MongoClient('mongodb://172.29.7.234:27017/')

    db = mongoClient.admin
    db.authenticate('mongo', 'zrgwmx', mechanism='SCRAM-SHA-1')

    mongoDb = mongoClient['paper']

    mongoColl2 = mongoDb['dblp_fields_3']

    es = Elasticsearch(['172.29.7.234:9200'])
    actions = []
    for item in tqdm(mongoColl2.find()):
        id = item['_id']
        item.pop('_id')
        action = {
            "_index": "dblp_fields",
            "_id": id,
            "_source": item
        }
        actions.append(action)

        if len(actions) == 10000:
            helpers.bulk(es, actions)
            del actions[0:len(actions)]
    helpers.bulk(es, actions)


def deleteEs():
    es = Elasticsearch(['localhost:9200'])
    es.indices.delete('dblp_fields')


if __name__ == '__main__':
    # es = Elasticsearch(['172.29.7.234:9200'])
    # res = es.indices.delete('dblp_fields')
    # print(res)
    # processField()
    toEs()
    # deleteEs()
    # processField_3()
    # processField_4()

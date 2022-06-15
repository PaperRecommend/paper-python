import pymongo
from tqdm import tqdm
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import math


def processAuthors():
    mongoClient = pymongo.MongoClient('mongodb://172.29.7.234:27017/')

    db = mongoClient.admin
    db.authenticate('mongo', 'zrgwmx', mechanism='SCRAM-SHA-1')

    mongoDb = mongoClient['paper']

    mongoColl2 = mongoDb['dblp2']

    mongoColl3 = mongoDb['dblp_authors']
    mongoColl4 = mongoDb['dblp_authors_2']

    for item in tqdm(mongoColl3.find({})):
        paperCount = 0
        citationCount = 0
        coauthors = []
        fields = {}
        years = {}
        if 'papers' in item.keys():
            paperCount = len(item['papers'])
            for paperId in item['papers']:
                paper = mongoColl2.find_one({'_id': paperId})
                if 'n_citation' in paper.keys():
                    citationCount = citationCount + paper['n_citation']
                if 'authors' in paper.keys():
                    for author in paper['authors']:
                        flag = False
                        for coauthor in coauthors:
                            if author['id'] == coauthor['id']:
                                coauthor['count'] = coauthor['count'] + 1
                                flag = True
                        if flag == False:
                            coauthors.append({'id': author['id'], 'name': author['name'], 'count': 1})
                if 'fos' in paper.keys():
                    for field in paper['fos']:
                        if field['name'] in fields:
                            fields[field['name']] = fields[field['name']] + 1
                        else:
                            fields[field['name']] = 1
                if 'year' in paper.keys():
                    year = str(paper['year'])
                    if year in years.keys():
                        years[year] = years[year] + 1
                    else:
                        years[year] = 1
        temp = {'coauthors': coauthors, 'fields': fields, 'years': years, 'paperCount': paperCount,
                'citationCount': citationCount}
        item.update(temp)
        mongoColl4.insert_one(item)


def processAuthors_2():
    mongoClient = pymongo.MongoClient('mongodb://172.29.7.234:27017/')

    db = mongoClient.admin
    db.authenticate('mongo', 'zrgwmx', mechanism='SCRAM-SHA-1')

    mongoDb = mongoClient['paper']

    mongoColl2 = mongoDb['dblp2']

    mongoColl3 = mongoDb['dblp_authors']
    mongoColl4 = mongoDb['dblp_authors_2']
    for item in tqdm(mongoColl3.find({})):
        citations = []
        if 'papers' in item.keys():
            for paperId in item['papers']:
                paper = mongoColl2.find_one({'_id': paperId})
                if 'n_citation' in paper.keys():
                    citations.append(paper['n_citation'])
        h_index = hIndex(citations)
        # 热度=求和(每一年的论文数/((2022-那一年)/3))

        # 被引用率=被引用数/论文数
        cited_rate = item['citationCount'] / item['paperCount']

        # 综合指数
        # composite_index=h_index*0.6+heat*0.4

        sum = 0
        for syear in item['years'].keys():
            year = int(syear)
            sum = sum + math.ceil(item['years'][syear] / ((2022 - year) / 3))

        heat = sum
        item.update({
            'h_index': h_index,
            'heat': heat,
            'cited_rate': cited_rate
        })
        mongoColl4.insert_one(item)


def processAuthors_3():
    mongoClient = pymongo.MongoClient('mongodb://172.29.7.234:27017/')

    db = mongoClient.admin
    db.authenticate('mongo', 'zrgwmx', mechanism='SCRAM-SHA-1')

    mongoDb = mongoClient['paper']

    mongoColl2 = mongoDb['dblp2']

    mongoColl3 = mongoDb['dblp_authors_2']
    mongoColl4 = mongoDb['dblp_authors']
    for item in tqdm(mongoColl3.find({})):

        if 'fields' in item.keys():
            fields = []
            fields_temp = item['fields']
            for key in fields_temp.keys():
                fields.append({'name': key, 'count': fields_temp[key]})
            fields = sorted(fields, key=lambda x: x['count'], reverse=True)
            if len(fields) > 10:
                fields = fields[:10]
            item.pop('fields')
            item.update({
                'fields': fields
            })
        mongoColl4.insert_one(item)


def hIndex(citations):
    """
    :type citations: List[int]
    :rtype: int
    """
    N = len(citations)
    citations.sort()
    h = 0
    for i, c in enumerate(citations):
        h = max(h, min(N - i, c))
    return h


def toEs():
    mongoClient = pymongo.MongoClient('mongodb://172.29.7.234:27017/')

    db = mongoClient.admin
    db.authenticate('mongo', 'zrgwmx', mechanism='SCRAM-SHA-1')

    mongoDb = mongoClient['paper']

    mongoColl2 = mongoDb['dblp_authors']

    es = Elasticsearch(['localhost:9200'])
    actions = []
    for item in tqdm(mongoColl2.find()):
        id = item['_id']
        item.pop('_id')
        action = {
            "_index": "dblp_authors",
            "_id": id,
            "_source": item
        }
        actions.append(action)

        if len(actions) == 10000:
            helpers.bulk(es, actions)
            del actions[0:len(actions)]
    helpers.bulk(es, actions)


if __name__ == '__main__':
    # es = Elasticsearch(['localhost:9200'])
    # # # es.indices.create(index='dblp_authors')
    # # # processAuthors_2()
    # res = es.indices.delete('dblp_authors')
    # print(res)
    toEs()
    # processAuthors_3()

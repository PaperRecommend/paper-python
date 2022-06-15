# for au_paper in mongoColl4.find({'authors': {'$elemMatch': {'id': auId}}}):
#     papers.append(au_paper['_id'])

import pymongo
import json
from tqdm import tqdm
import numpy as np
mongoClient = pymongo.MongoClient('mongodb://172.29.7.234:27017/')

db = mongoClient.admin
db.authenticate('mongo', 'zrgwmx', mechanism='SCRAM-SHA-1')

mongoDb = mongoClient['paper']
mongoColl=mongoDb['dblp']
mongoColl3=mongoDb['dblp3']
mongoColl4=mongoDb['dblp4']
mongoCollAR=mongoDb['dblp_authors']
mongoCollField=mongoDb['dblp_fields']
mongoCollVenue=mongoDb['dblp_venues']
def get_mongo_dblp_authors():
    for item in tqdm(mongoColl4.find()):
        for author in item['authors']:
            auId= author['id']
            meta=mongoCollAR.find_one({'_id': auId})
            if meta ==None :
                meta={'_id': auId}
                co_author=author
                co_author.pop('id')
                meta.update(co_author)
                papers=[]
                papers.append(item['_id'])
                meta.update({'papers':papers})
                mongoCollAR.insert_one(meta)
            else:
                papers=meta['papers']
                papers.append(item['_id'])
                mongoCollAR.update_one({'_id': auId},{ "$set": { "papers": papers}})

def testFOS():
    fields={}
    for item in tqdm(mongoColl4.find()):
        for field in item['fos']:
            fieldName=field['name']
            if fieldName not in fields.keys():
                fields[fieldName]=[]
            temp={'id':item['_id'],'w':field['w']}
            fields[fieldName].append(temp)
    id=0
    for key in tqdm(fields):
        id+=1
        item={'_id': id,'paper': fields[key]}
        mongoCollField.insert_one(item)
    print(len(fields))
def testVenue():
    dic={}
    for item in tqdm(mongoColl4.find()):
        if 'venue' not in item.keys():
            continue
        venue=item['venue']
        if 'id' not in venue:
            continue
        vId=venue['id']
        venue.pop('id')
        if vId not in dic.keys():
            dic[vId]=venue
            dic[vId].update({'papers':[]})
        dic[vId]['papers'].append(item['_id'])
    for vId in dic:
        temp={'_id':vId}
        temp.update(dic[vId])
        mongoCollVenue.insert_one(temp)
testVenue()
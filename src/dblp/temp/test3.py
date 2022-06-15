
import pymongo
import json
import numpy as np
from tqdm import tqdm

mongoClient = pymongo.MongoClient('mongodb://172.29.7.234:27017/')

db = mongoClient.admin
db.authenticate('mongo', 'zrgwmx', mechanism='SCRAM-SHA-1')

mongoDb = mongoClient['paper']
mongoColl=mongoDb['dblp']
mongoColl3=mongoDb['dblp3']
mongoColl4=mongoDb['dblp4']

# citDict: {'Eid': {'citingDocEid':citingDocEid, 'citedDocEid':citedDocEid, 'coCitedDocEidLst':coCitedDocEidLst, 'txt':txt}}

def getIdArrayJson():
    idArray=[]
    for item in mongoColl3.find({}):
        idArray.append(item['id'])
    idArrayNP=np.array(idArray)
    np.save("../../data/dblp/idArray.npy",idArrayNP)
    idArray=np.load("../../data/dblp/idArray.npy")
def getMetaDictJson():
    idDict = {}
    count = 0
    for item in mongoColl4.find({}):
        count+=1
        if 'venue' in item.keys():
            idDict[item['_id']]={'Eid':item['_id'],'title':item['title'],'year':item['year'],'journal':item['venue']['raw']}
        else:
            idDict[item['_id']] = {'Eid': item['_id'], 'title': item['title'], 'year': item['year'],'journal': ''}
        authors=[]
        for author in item['authors']:
            authors.append(author['name'])
        author=''
        if authors !=[]:
            author=','.join(authors)
        idDict[item['_id']].update({'author':author})
        if count%10000==0:
            print(count)

    with open('../../data/dblp/metaDict2.json','w',encoding='utf8') as fp:
        json.dump(idDict,fp)
def changeId():
    for item in tqdm(mongoColl.find()):
        dict = {'_id': item['id']}
        item.pop('id')
        item.pop('_id')
        dict.update(item)
        mongoColl3.insert_one(dict)

getMetaDictJson()




import pymongo
import dblpCorpus as corpus
from tqdm import tqdm
import json
import numpy as np
# a=5
# dic={}
# dic[a]=10
# print(dic[5])
import pymongo

mongoClient = pymongo.MongoClient('mongodb://172.29.7.234:27017/')

db = mongoClient.admin
db.authenticate('mongo', 'zrgwmx', mechanism='SCRAM-SHA-1')

mongoDb = mongoClient['paper']
mongoColl = mongoDb['dblp']
mongoColl3 = mongoDb['dblp3']
for item in tqdm(mongoColl3.find()):
    if 'indexed_abstract' in item.keys():
        length=item['indexed_abstract']['IndexLength']
        abstractArray=["" for i in range(0,length)]
        indexAbstract=item['indexed_abstract']['InvertedIndex']
        for key in indexAbstract.keys():
            for index in indexAbstract[key]:
                abstractArray[index]=key
        abstract=" ".join(abstractArray)
        item.pop('indexed_abstract')
        item['abstract']=abstract
        mongoColl.insert_one(item)


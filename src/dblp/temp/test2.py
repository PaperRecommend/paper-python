import pymongo
import json
import numpy as np
from multiprocessing import Pool as mPool
from multiprocessing.dummy import Pool as dPool

def getcitDict():
    mongoClient = pymongo.MongoClient('mongodb://172.29.7.234:27017/')

    db = mongoClient.admin
    db.authenticate('mongo', 'zrgwmx', mechanism='SCRAM-SHA-1')

    mongoDb = mongoClient['paper']
    mongoColl3=mongoDb['dblp3']

    # citDict: {'Eid': {'citingDocEid':citingDocEid, 'citedDocEid':citedDocEid, 'coCitedDocEidLst':coCitedDocEidLst, 'txt':txt}}
    citDict={}
    count=0

    # for item in mongoColl3.find({}):
    #     idArray.append(item['id'])
    # idArrayNP=np.array(idArray)
    # np.save("../../data/dblp/idArray.npy",idArrayNP)
    idArray=np.load("../../data/dblp/idArray.npy")
    for item in mongoColl3.find({}):
        count+=1
        for refId in item['references']:
            if refId in idArray:
                if refId not in citDict.keys():
                    citDict[refId]=[]
                citDict[refId].append(item['id'])
        if count%1000==0:
            print(count)
    print(len(citDict))
    with open('../../data/dblp/dblp3.json',mode='w',encoding='utf8') as fp:
        json.dump(citDict,fp)
getcitDict()

# def main(mstart):
#     dpool = dPool(processes=5)
#     dpool.map(getcitDict,[mstart+i * 20000 for i in range(0,5)])
#
# if __name__ == '__main__':
#     mpool = mPool(processes=5)
#     result=mpool.map(main, [i * 100000 for i in range(0,5)])
#
#     mpool.close()
#     mpool.join()
#     citDict={}
#     for dict in result:
#         citDict.update(dict)
#     print("citDict")
#     print(len(citDict))
#     with open('../../data/dblp/dblp3.json',mode='w',encoding='utf8') as fp:
#         json.dump(citDict,fp)
# print(len(dict))
# refItem=mongoColl3.find_one({'id': 1974211698})
# if refItem is None:
#     print("no")
# arr=[1,2,3]
# arr2=np.array(arr)
# if 2 in arr2:
#     print("good")


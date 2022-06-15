import time
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import pymongo

# mongoClient = pymongo.MongoClient('mongodb://172.29.7.234:27017/')
#
# db = mongoClient.admin
# db.authenticate('mongo', 'zrgwmx', mechanism='SCRAM-SHA-1')
#
# mongoDb = mongoClient['paper']
# mongoColl=mongoDb['dblp']
# mongoColl2=mongoDb['dblp2']
# mongoColl4=mongoDb['dblp4']
#
# start_time = time.time()
# es = Elasticsearch(['localhost:9200'])
# actions = []
# i=0
# for item in tqdm(mongoColl2.find()):
#     id=item['_id']
#     item.pop('_id')
#     action = {
#         "_index": "dblp",
#         "_id": id,
#         "_source": item
#     }
#     i += 1
#     actions.append(action)
#     if len(actions) == 1000:
#         helpers.bulk(es, actions)
#         del actions[0:len(actions)]
#
# if i > 0:
#     helpers.bulk(es, actions)
#
# end_time = time.time()
# t = end_time
# print('本次共写入{}条数据，用时{}s'.format(i, t))
# body = {
#   "query":{
#     "match":{"_id":13407}
#   }
# }
# print(es.search(index="dblp2",body=body))
# es.indices.create(index='dblp')
# es.indices.delete(index='dblp', ignore=[400, 404])
# body = {
#     "query":{
#         "match_all":{}
#     }
# }
# print(es.count(index='dblp'))
a = [1, 2, 3]
print(a[:4])

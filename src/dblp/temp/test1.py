
import pymongo
mongoClient = pymongo.MongoClient('mongodb://172.29.7.234:27017/')

db = mongoClient.admin
db.authenticate('mongo', 'zrgwmx', mechanism='SCRAM-SHA-1')

mongoDb = mongoClient['paper']
mongoColl = mongoDb['dblp']
mongoColl2=mongoDb['dblp2']
mongoColl3=mongoDb['dblp3']

sum=set()
count=0
print('good')
# for pap in mongoColl.find({"keywords": {'$exists': True},"year": {"$gte": 2013}}):
#     count=count+1
#     for ref in pap['references']:
#         sum.add(ref)
#     if(count%10000)==0:
#         print(count)
# print(count)
# print('数量')
# print(len(sum))
# for item in mongoColl3.find({"fos": {'$exists': True},"references": {'$exists': True}}):
#     count+=1
#     if(count%10000)==0:
#         print(count)
# print(count)
#
# for item in mongoColl2.find({"fos": {'$exists': True},"references": {'$exists': True},"n_citation":{"$gte":5}}):
#     count+=1
#     mongoColl3.insert_one(item)
#     if(count%10000)==0:
#         print(count)
# print(count)
a={"abc":[1,2,3,4,5,6]}
b={"abc":[8,9,10,11,12]}
a.update(b)
print(a)

    # pre+=1
    # for ref in item['references']:
    #     if mongoColl2.find({"id": ref}) is None:
    #         count+=1
    # if(pre%10000)==0:
    #     print(pre)


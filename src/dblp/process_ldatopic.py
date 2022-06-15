import pymongo
import text
import nltk
from tqdm import tqdm

mongoClient = pymongo.MongoClient('mongodb://172.29.7.234:27017/')

db = mongoClient.admin
db.authenticate('mongo', 'zrgwmx', mechanism='SCRAM-SHA-1')

mongoDb = mongoClient['paper']
mongoColl = mongoDb['ldaTopic']
mongoColl2 = mongoDb['lda_topics']

mongoColl3 = mongoDb['dblp2']
topTokCnt = 20


def process_topics():
    for topic in tqdm(mongoColl.find({})):
        sentences = []
        tokExptFreq = {}
        term = []

        for doc in topic["doc"]:

            paper = mongoColl3.find_one({'_id': doc['id']})
            fos = [item["name"] for item in paper['fos']]
            title = paper['title']

            tokLst = text.wordTokenize(title, rmStopwordsOption=True)
            temp = []
            for field in fos:
                temp.extend(text.wordTokenize(field))
            tokLst.extend(temp)

            for tok in tokLst:
                tokExptFreq[tok] = tokExptFreq.get(tok, 0.0) + doc['w']
        topTokId = [t for t in sorted(tokExptFreq, key=lambda x: tokExptFreq[x], reverse=True)][0:topTokCnt]
        topToks = [(tokExptFreq[tok], tok) for tok in topTokId]
        term = [{"name": item[1], "w": item[0]} for item in topToks]
        topic.pop('term')
        topic.update({
            'term': term
        })
        mongoColl2.insert_one(topic)


if __name__ == '__main__':
    # stopwordsLst = nltk.corpus.stopwords.words('english')
    # print(stopwordsLst)
    process_topics()

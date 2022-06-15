from recommend.cal_similarity import EntitySimilartiy
from recommend.get_user_actions import get_user_click_collect, get_user_search, get_user_click_collect_keywords, \
    get_user_search_keywords
import pymongo
from elasticsearch import Elasticsearch
from tqdm import tqdm


def es_iterate_all_documents(es, index, pagesize=250, scroll_timeout="10m", **kwargs):
    """
    Helper to iterate ALL values from a single index
    Yields all the documents.
    """
    is_first = True
    while True:
        # Scroll next
        if is_first:  # Initialize scroll
            result = es.search(index=index, scroll="10m", **kwargs, body={
                "size": pagesize
            })
            is_first = False
        else:
            result = es.scroll(body={
                "scroll_id": scroll_id,
                "scroll": scroll_timeout
            })
        scroll_id = result["_scroll_id"]
        hits = result["hits"]["hits"]
        # Stop after no more docs
        if not hits:
            break
        # Yield each entry
        yield from (hit['_source'] for hit in hits)


def save_to_es(es, index_name, uid, content):
    searchBody = {
        "query": {
            "match": {
                "id": uid
            }
        }
    }
    user_recommend = es.search(index=index_name, body=searchBody)
    hit = user_recommend["hits"]["hits"]
    if len(hit) == 1:
        es.delete(index=index_name, id=uid)
    action = {
        "id": uid,
        "papers": content

    }
    es.index(index=index_name,id=uid, body=action)


def main_single(uid):
    entitySimilarity = EntitySimilartiy()
    es = Elasticsearch(['localhost:9200'])
    word_nums = get_user_click_collect(es, uid)
    word_nums.extend(get_user_search(es, uid))
    word_nums = sorted(word_nums, key=lambda x: x[1], reverse=True)[:30]
    sim_topics = entitySimilarity.get_sim_topics(word_nums)[:3]

    mongoClient = pymongo.MongoClient('mongodb://172.29.7.234:27017/')

    db = mongoClient.admin
    db.authenticate('mongo', 'zrgwmx', mechanism='SCRAM-SHA-1')

    mongoDb = mongoClient['paper']
    mongoColl = mongoDb['ldaTopic']

    weight = [30, 20, 10]
    docIds = []
    for i in range(len(sim_topics)):
        topic = mongoColl.find_one({"_id": sim_topics[i][0]})
        docs = sorted(topic["doc"], key=lambda x: x['w'], reverse=True)[:weight[i]]
        docIds = docIds + [doc['id'] for doc in docs]
    save_to_es(es, 'user_lda_recommend', uid, docIds)


def main_all():
    entitySimilarity = EntitySimilartiy()
    es = Elasticsearch(['localhost:9200'])
    mongoClient = pymongo.MongoClient('mongodb://172.29.7.234:27017/')

    db = mongoClient.admin
    db.authenticate('mongo', 'zrgwmx', mechanism='SCRAM-SHA-1')

    mongoDb = mongoClient['paper']
    mongoColl = mongoDb['ldaTopic']
    weight = [30, 20, 10]
    for user in tqdm(es_iterate_all_documents(es, 'user_search')):
        word_nums = get_user_click_collect(es, user['id'])
        word_nums.extend(get_user_search_keywords(user))
        word_nums = sorted(word_nums, key=lambda x: x[1], reverse=True)[:30]
        sim_topics = entitySimilarity.get_sim_topics(word_nums)[:3]

        docIds = []
        for i in range(len(sim_topics)):
            topic = mongoColl.find_one({"_id": sim_topics[i][0]})
            docs = sorted(topic["doc"], key=lambda x: x['w'], reverse=True)[:weight[i]]
            docIds = docIds + [doc['id'] for doc in docs]
        save_to_es(es, 'user_lda_recommend', user['id'], docIds)


if __name__ == '__main__':
    # es = Elasticsearch(['localhost:9200'])
    # es.indices.create(index='user_lda_recommend')
    main_all()
    # for user in tqdm(es_iterate_all_documents(es, 'user_action')):
    #     print(user)

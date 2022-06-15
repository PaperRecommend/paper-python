from elasticsearch import Elasticsearch
import time
from recommend.util import process_string, count_word_frequency

es = Elasticsearch(['localhost:9200'])


def es_iterate_all_documents(index, pagesize=250, scroll_timeout="1m", **kwargs):
    """
    Helper to iterate ALL values from a single index
    Yields all the documents.
    """

    global es
    is_first = True
    while True:
        # Scroll next
        if is_first:  # Initialize scroll
            result = es.search(index=index, scroll="1m", **kwargs, body={
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


month_weight = [5, 4, 3, 2, 1]
one_month = 24 * 60 * 60 * 30 * 1000
months = [one_month, one_month * 2, one_month * 3, one_month * 4, one_month * 5]


def get_month_weight_index(action_time, current_time):
    global months
    for i in range(len(months)):
        if months[i] > current_time - action_time:
            return i
    return -1


def get_paper_titles(papers):
    global es
    paper_titles = []
    for paper_id in papers:
        body = {
            "query": {
                "match": {
                    "_id": paper_id
                }
            }
        }
        result = es.search(index="dblp", body=body)
        hit = result["hits"]["hits"]
        if len(hit) == 1:
            title = hit[0]['_source']['title']
            paper_titles.append(title)
    return paper_titles


def get_word_nums_list(paper_ids, action_weight):
    global month_weight
    paper_titles = []
    for arr in paper_ids:
        paper_titles.append(get_paper_titles(arr))

    word_nums_all = []
    for i in range(len(paper_titles)):
        word_list = []
        for sentence in paper_titles[i]:
            token_list = process_string(sentence)
            word_list.extend(token_list)
        word_nums = count_word_frequency(word_list, action_weight + month_weight[i])
        word_nums_all.extend(word_nums)
    return word_nums_all


def get_user_click_collect_keywords(user):
    current_time = int(round(time.time() * 1000))

    click_paper_ids = [[] for i in range(5)]
    for item in user['clickActions']:
        weight_index = get_month_weight_index(item['lastTime'], current_time)
        if weight_index != -1:
            click_paper_ids[weight_index].append(item['paperId'])
    word_nums_all = get_word_nums_list(click_paper_ids, 1)

    collect_paper_ids = [[] for i in range(5)]
    for item in user['paperCollections']:

        weight_index = get_month_weight_index(item['lastTime'], current_time)
        if weight_index != -1:
            collect_paper_ids[weight_index].append(item['paperId'])

    word_nums_all.extend(get_word_nums_list(collect_paper_ids, 2))

    word_nums = sorted(word_nums_all, key=lambda x: x[1], reverse=True)[:30]

    return word_nums


def get_all_users_click_collect():
    for user in es_iterate_all_documents('user_action'):
        word_nums = get_user_click_collect_keywords(user)
        yield word_nums


def get_user_search_keywords(user):
    global month_weight, es

    current_time = int(round(time.time() * 1000))

    search_strings = [[] for i in range(5)]
    for item in user['searchActions']:
        weight_index = get_month_weight_index(item['lastTime'], current_time)
        if weight_index != -1:
            search_strings[weight_index].append(item['searchContent'])
    word_nums_all = []

    for i in range(len(search_strings)):
        word_list = []
        for sentence in search_strings[i]:
            token_list = process_string(sentence)
            word_list.extend(token_list)
        word_num = count_word_frequency(word_list, 3 + month_weight[i])
        word_nums_all.extend(word_num)
    word_nums = sorted(word_nums_all, key=lambda x: x[1], reverse=True)[:30]
    return word_nums


def get_all_users_search(uid):
    for user in es_iterate_all_documents('user_search'):
        word_nums = get_user_click_collect_keywords(user)
        yield word_nums,user["_id"]


if __name__ == '__main__':
    millis = int(round(time.time() * 1000))

    # for entry in es_iterate_all_documents(es, 'user_action'):
    #     print(entry)

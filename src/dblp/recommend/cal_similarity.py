# coding: utf-8


import os
from gensim.models import Word2Vec
import pymongo


class EntitySimilartiy:
    def __init__(self):

        print("初始化成功")
        self.model = Word2Vec.load('paper_model')
        mongoClient = pymongo.MongoClient('mongodb://172.29.7.234:27017/')

        db = mongoClient.admin
        db.authenticate('mongo', 'zrgwmx', mechanism='SCRAM-SHA-1')

        mongoDb = mongoClient['paper']
        self.mongoColl = mongoDb['ldaTopic']

    def editDistanceDP(self, s1, s2):
        """
        采用DP方法计算编辑距离
        :param s1:
        :param s2:
        :return:
        """
        m = len(s1)
        n = len(s2)
        solution = [[0 for j in range(n + 1)] for i in range(m + 1)]
        for i in range(len(s2) + 1):
            solution[0][i] = i
        for i in range(len(s1) + 1):
            solution[i][0] = i

        for i in range(1, m + 1):
            for j in range(1, n + 1):
                if s1[i - 1] == s2[j - 1]:
                    solution[i][j] = solution[i - 1][j - 1]
                else:
                    solution[i][j] = 1 + min(solution[i][j - 1], min(solution[i - 1][j],
                                                                     solution[i - 1][j - 1]))
        return solution[m][n]

    def simCal(self, word, entities):
        """
        计算词语和字典中的词的相似度
        相同字符的个数/min(|A|,|B|)   +  余弦相似度
        """

        a = len(word)
        scores = []
        for entity in entities:
            sim_num = 0
            b = len(entity['name'])
            c = len(set(entity['name'] + word))
            temp = []
            for w in word:
                if w in entity['name']:
                    sim_num += 1
            if sim_num != 0:
                score1 = sim_num / c  # overlap score
                temp.append(score1)
            try:
                score2 = self.model.wv.similarity(word, entity['name'])  # 余弦相似度分数
                temp.append(score2)
            except:
                pass
            score3 = 1 - self.editDistanceDP(word, entity['name']) / (a + b)  # 编辑距离分数
            if score3:
                temp.append(score3)

            score = sum(temp) / len(temp)

            scores.append((entity['name'], score))

        scores = sorted(scores, key=lambda k: k[1], reverse=True)[:2]
        return sum([item[1] for item in scores])

    def get_sim_topics(self, words):
        all_score = 0
        sim_topics = []
        for topic in self.mongoColl.find({}):
            entities = []
            score = 0
            if 'term' in topic.keys():
                for word_tuple in words:
                    score = self.simCal(word_tuple[0], topic['term']) * word_tuple[1]
                    sim_topics.append((topic["_id"], score))
        sim_topics = sorted(sim_topics, key=lambda x: x[1], reverse=True)
        return sim_topics


if __name__ == '__main__':
    entitySimilarity = EntitySimilartiy()
    score = entitySimilarity.get_sim_topics(['http', 'network'])
    print(score)

# coding: utf-8


import os
from gensim.models import Word2Vec
import pymongo


mongoClient = pymongo.MongoClient('mongodb://172.29.7.234:27017/')

db = mongoClient.admin
db.authenticate('mongo', 'zrgwmx', mechanism='SCRAM-SHA-1')

mongoDb = mongoClient['paper']
mongoColl=mongoDb['ldaTopic']


data_dir = "./data_entity/"


class EntitySimilartiy:
    def __init__(self):


        print("初始化成功")
        self.model =Word2Vec.load('paper_model')
        mongoClient = pymongo.MongoClient('mongodb://172.29.7.234:27017/')

        db = mongoClient.admin
        db.authenticate('mongo', 'zrgwmx', mechanism='SCRAM-SHA-1')

        mongoDb = mongoClient['paper']
        self.mongoColl = mongoDb['ldaTopic']


        print("模型加载成功")

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

        print("计算相似")

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
                score1 = sim_num / c + entity['w'] # overlap score
                temp.append(score1)
            try:
                score2 = self.model.wv.similarity(word, entity['name']) + entity['w']  # 余弦相似度分数
                temp.append(score2)
            except:
                pass
            score3 = 1 - self.editDistanceDP(word, entity['name']) / (a + b) ++ entity['w']  # 编辑距离分数
            if score3:
                temp.append(score3)

            score = sum(temp) / len(temp)
            if score >= 0.5:
                scores.append((entity['name'], score))

        scores.sort(key=lambda k: k[1], reverse=True)
        return scores

    def find_sim_words(self, word, entities):
        """
        当全匹配失败时，就采用相似度计算来找相似的词
        :param question:
        :return:
        """

        print("找相似")
        # 找到前三个最想死的词，将分数相加
        scores=self.simCal(word,entities)
        sumScore=0
        if(len(scores)>3):
            sumScore=sum([item[1] for item in scores[:3]])
        else:
            sumScore = sum([item[1] for item in scores])


        return sumScore

    def cal_sim_words(self, word):

        item=self.mongoColl.find_one({'_id':143})
        entities=[]
        score=0
        if 'term' in item.keys():
            score=self.find_sim_words(word,item['term'])
        return score


if __name__ == '__main__':
    entitySimilarity=EntitySimilartiy()
    score=entitySimilarity.cal_sim_words('http')
    print(score)

# while True:
#     word=input("请输入实体: ")
#     label=input("请输入类型: ")
#     if label=="1":
#         labels=['disease']
#     elif label=="2":
#         labels=['symptom']
#     elif label=="3":
#         labels=['check']
#     elif label=="4":
#         labels=['drug']
#     elif label=="5":
#         labels=['food']
#     result=entitySimilarity.find_sim_words(word,labels)
#     print("result: ")
#     print(result)
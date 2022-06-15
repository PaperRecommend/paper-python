# coding: utf-8


import os
from gensim.models import KeyedVectors

data_dir="./data_entity/"
class EntitySimilartiy:
    def __init__(self):
        cur_dir = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        # 路径
        # self.vocab_path = os.path.join(cur_dir, 'data/vocab.txt')
        # self.stopwords_path =data_dir+'stop_words.utf8'
        self.word2vec_path = data_dir+'merge_sgns_bigram_char300.txt'

        self.result={}

        #实体文件路径
        self.disease_path = data_dir + 'disease.txt'
        self.symptom_path = data_dir + 'symptoms.txt'
        self.check_path = data_dir + 'check.txt'
        self.drug_path = data_dir + 'drug.txt'
        self.food_path = data_dir + 'food.txt'
        self.producer_path = data_dir + 'producer.txt'
        self.department_path = data_dir + 'department.txt'


        #获得所有实体
        self.disease_entities = [w.strip() for w in open(self.disease_path, encoding='utf8') if w.strip()]
        self.symptom_entities = [w.strip() for w in open(self.symptom_path, encoding='utf8') if w.strip()]
        self.check_entities = [w.strip() for w in open(self.check_path, encoding='utf8') if w.strip()]
        self.drug_entities = [w.strip() for w in open(self.drug_path, encoding='utf8') if w.strip()]
        self.food_entities = [w.strip() for w in open(self.food_path, encoding='utf8') if w.strip()]
        self.producer_entities = [w.strip() for w in open(self.producer_path, encoding='utf8') if w.strip()]
        self.department_entities = [w.strip() for w in open(self.department_path, encoding='utf8') if w.strip()]

        print("初始化成功")
        self.model = KeyedVectors.load_word2vec_format(self.word2vec_path, binary=False)

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

    def simCal(self, word, entities,flag):
        """
        计算词语和字典中的词的相似度
        相同字符的个数/min(|A|,|B|)   +  余弦相似度
        """

        print("计算相似")

        a = len(word)
        scores = []
        for entity in entities:
            sim_num = 0
            b = len(entity)
            c = len(set(entity+word))
            temp = []
            for w in word:
                if w in entity:
                    sim_num += 1
            if sim_num != 0:
                score1 = sim_num / c  # overlap score
                temp.append(score1)
            try:
                score2 = self.model.similarity(word, entity)  # 余弦相似度分数
                temp.append(score2)
            except:
                pass
            score3 = 1 - self.editDistanceDP(word, entity) / (a + b)  # 编辑距离分数
            if score3:
                temp.append(score3)

            score = sum(temp) / len(temp)
            if score >= 0.5:
                scores.append((entity, score, flag))

        scores.sort(key=lambda k: k[1], reverse=True)
        return scores

    def find_sim_words(self, word,labels):
        """
        当全匹配失败时，就采用相似度计算来找相似的词
        :param question:
        :return:
        """

        print("找相似")
        alist=[]
        temp = []

        for label in labels:
            if label=="disease":
                scores = self.simCal(word, self.disease_entities, label)
                alist.extend(scores)


            elif label=="symptom":
                scores = self.simCal(word, self.symptom_entities, label)
                alist.extend(scores)

            elif label=="check":
                scores = self.simCal(word, self.check_entities, label)
                alist.extend(scores)
            elif label=="department":
                scores = self.simCal(word, self.department_entities, label)
                alist.extend(scores)
            elif label=="drug":
                scores = self.simCal(word, self.drug_entities, label)
                alist.extend(scores)
            elif label=="food":
                scores = self.simCal(word, self.food_entities, label)
                alist.extend(scores)
            elif label=="producer":
                scores = self.simCal(word, self.producer_entities, label)
                alist.extend(scores)


        temp1 = list(sorted(alist, key=lambda k: k[1], reverse=True))
        return temp1


    def get_sim_words(self, word,labels):
        result=self.find_sim_words(word,labels)
        all_label=['disease','symptom','check','drug','food','department','producer']
        tmp_labels=[label for label in all_label if label not in labels]
        if result==[] or result[0][1]<0.75:
            tmp_res=self.find_sim_words(word,tmp_labels)
            result=result+tmp_res

        args={}
        flag=True
        result=list(sorted(result, key=lambda k: k[1], reverse=True))
        if result==[]:
            flag=False
            return args,flag
        else:
            if(result[0][1]>0.75):
                args[result[0][0]]=[result[0][2]]
            else:
                if len(result)>3:
                    result=result[:3]

                flag=False
                for tup in result:
                    if(tup[1]>0.58):
                        args[tup[0]]=[tup[2]]

        return args,flag




if __name__ == '__main__':
    entitySimilarity=EntitySimilartiy()
    labels=[]
    res_classify={'args': {'头疼': ['symptom']}, 'question_types': ['disease_symptom']}
    dict=res_classify['args']
    result={}
    result['question_types']=res_classify['question_types']
    args={}
    for key in dict.keys():
        tmp=entitySimilarity.get_sim_words(key,dict[key])
        args.update(tmp)
    result['args']=args
    print(result)

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
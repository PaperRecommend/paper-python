
from gensim.models import Word2Vec

model = Word2Vec.load('paper_model')     #模型讀取方式
# model.most_similar(positive=['woman', 'king'], negative=['man']) #根据给定的条件推断相似词
print(model.wv.most_similar('model'))
# model.doesnt_match("breakfast cereal dinner lunch".split()) #寻找离群词
print(model.wv.similarity('approach', 'method')) #计算两个单词的相似度
# model['computer'] #获取单词的词向量


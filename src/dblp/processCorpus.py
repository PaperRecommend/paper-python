import pymongo
from tqdm import tqdm
import nltk
import nltk.tokenize
import nltk.stem
import nltk.chunk
from nltk.tokenize import RegexpTokenizer
from nltk import pos_tag
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet
from nltk.tokenize import sent_tokenize

wnl = WordNetLemmatizer()

tokenizer = RegexpTokenizer(r'\w+')

stemmer = nltk.stem.PorterStemmer()
snowballStemmer = nltk.stem.SnowballStemmer('english')
stopwordsLst = nltk.corpus.stopwords.words('english')
stopwordsSet = set(stopwordsLst)

mongoClient = pymongo.MongoClient('mongodb://172.29.7.234:27017/')

db = mongoClient.admin
db.authenticate('mongo', 'zrgwmx', mechanism='SCRAM-SHA-1')

mongoDb = mongoClient['paper']

mongoColl2 = mongoDb['dblp']



# 获取单词的词性
def get_wordnet_pos(tag):
    if tag.startswith('J'):
        return wordnet.ADJ
    elif tag.startswith('V'):
        return wordnet.VERB
    elif tag.startswith('N'):
        return wordnet.NOUN
    elif tag.startswith('R'):
        return wordnet.ADV
    else:
        return None

index = 0
text = []
for item in tqdm(mongoColl2.find().skip(2000000)):
    index = index + 1

    if 'abstract' in item.keys():

        abstract = item['abstract'].lower()
        sentLst = sent_tokenize(abstract)
        for sentence in sentLst:
            tokenLst = tokenizer.tokenize(sentence)
            tokenLst = [tok for tok in tokenLst if (tok not in stopwordsSet)]
            # tokenLst = [snowballStemmer.stem(tok) for tok in tokenLst]
            for i in range(len(tokenLst)):
                tag = get_wordnet_pos(pos_tag([tokenLst[i]])[0][1])
                if tag is not None:
                    tokenLst[i] = wnl.lemmatize(tokenLst[i], tag)
            text.append(tokenLst)

    if index % 100000 == 0:

        with open("../../data/dblp/bigCorpus2.txt", encoding='utf8', mode='a+') as fp:
            for line in text:
                # print(" ".join(line))
                fp.write(" ".join(line))
                fp.write("\n")
            fp.close()
        text = []
    if index==2000000:
        break


# # 分别定义需要进行还原的单词与相对应的词性
# words = ['cars', 'men', 'running', 'ate', 'saddest', 'fancier']
# for i in range(len(words)):
#     wnl.lemmatize(words[i],get_wordnet_pos(pos_tag([words[i]])[0][1]))

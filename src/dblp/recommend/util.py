import nltk
import nltk.tokenize
import nltk.stem
import nltk.chunk
from nltk.tokenize import RegexpTokenizer
from nltk import pos_tag
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet
import math
from nltk.tokenize import sent_tokenize

wnl = WordNetLemmatizer()
tokenizer = RegexpTokenizer(r'\w+')
stemmer = nltk.stem.PorterStemmer()
snowballStemmer = nltk.stem.SnowballStemmer('english')
stopwordsLst = nltk.corpus.stopwords.words('english')
stopwordsSet = set(stopwordsLst)


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


def process_string(sentence):
    sentence = sentence.lower()
    token_list = tokenizer.tokenize(sentence)
    token_list = [tok for tok in token_list if (tok not in stopwordsSet)]

    for i in range(len(token_list)):
        tag = get_wordnet_pos(pos_tag([token_list[i]])[0][1])
        if tag is not None:
            token_list[i] = wnl.lemmatize(token_list[i], tag)

    return token_list


def count_word_frequency(words, weight):
    word_dict = {}
    for word in words:
        if word in word_dict.keys():
            word_dict[word] = word_dict[word] + 1
        else:
            word_dict[word] = 1
    for key in word_dict.keys():
        word_dict[key] = word_dict[key] * weight
    word_nums = sorted(list(word_dict.items()), key=lambda x: x[1], reverse=True)
    return word_nums

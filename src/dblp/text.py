'''
Created on Feb 1, 2013

@author: xwang95
'''
import nltk
import nltk.tokenize
import nltk.stem
import nltk.chunk
from nltk import pos_tag
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet

stemmer = nltk.stem.PorterStemmer()
snowballStemmer = nltk.stem.SnowballStemmer('english')
stopwordsLst = nltk.corpus.stopwords.words('english')
stopwordsLst_2 = ['(', ')', ':', '-', '!', ';', '\"', '\'', '.', ',', '?', '/', '\\', '[', ']', '{', '}', '<', '>', '*',
                  '+', '&', '#', '~', '%', '@']
stopwordsSet = set(stopwordsLst + stopwordsLst_2)

wnl = WordNetLemmatizer()


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


# ===============================================================================
# Get: stopwords set
# ===============================================================================
def getStopwordsSet(): return stopwordsSet


# ===============================================================================
# Get: porter stemmer
# ===============================================================================
def getPorterStemmer(): return stemmer


# ===============================================================================
# stem (using porter stemmer)
# ===============================================================================
def stemWithPorterStemmer(tok): return stemmer.stem(tok)


# ===============================================================================
# stem (using snowball stemmer)
# ===============================================================================
def stemWithSnowballStemmer(tok): return str(snowballStemmer.stem(tok))


# ===============================================================================
# sentence boundary detection (doc -> sent)
# ===============================================================================
def sentTokenize(docStr): return nltk.sent_tokenize(docStr)


# ===============================================================================
# tokenize (sent -> token)
# ===============================================================================
def wordTokenize(sentStr, stemmingOption=False, rmStopwordsOption=True):
    sentStr = sentStr.lower()
    tokenLst = nltk.word_tokenize(sentStr)
    temp=[]
    for tok in tokenLst:
        if tok not in stopwordsSet:
            temp.append(tok)
    tokenLst=temp
    # if (rmStopwordsOption): tokenLst = [tok for tok in tokenLst if (tok not in getStopwordsSet())]
    # tokenLst = [stemWithSnowballStemmer(tok) for tok in tokenLst]
    for i in range(len(tokenLst)):
        tag = get_wordnet_pos(pos_tag([tokenLst[i]])[0][1])
        if tag is not None:
            tokenLst[i] = wnl.lemmatize(tokenLst[i], tag)
    return tokenLst


# ===============================================================================
# sentence boundary detection + tokenize (revome stopwords,  stemming)
# ===============================================================================
def preprocText(txt, stemmingOption, rmStopwordsOption):
    return [wordTokenize(sent, stemmingOption, rmStopwordsOption) for sent in sentTokenize(txt)]


# ===============================================================================
# postag (get list of pos-tag)
# ===============================================================================
def postag(tokenLst): return [pair[1] for pair in nltk.pos_tag(tokenLst)]


# ===============================================================================
# postag (get list of pairs [word, tag])
# ===============================================================================
def postagWordTagPair(tokenLst): return nltk.pos_tag(tokenLst)


# ===============================================================================
# convert fine postag to coarse tag
# ===============================================================================
def convertPostagFineToCoarse(tag):
    if (len(tag) > 2):
        return tag[0:2]
    else:
        return tag


# ===============================================================================
# postag (get list of COARSE postag)
# ===============================================================================
def postagCoarseTag(tokenLst): return [convertPostagFineToCoarse(tag) for tag in postag(tokenLst)]


# ===============================================================================
# get the NER chunks represented by trees
# ===============================================================================
def nerChunk(tokenLst, postagLst=None):
    if (not postagLst): postagLst = postag(tokenLst)
    tagged = [(tokenLst[i], postagLst[i]) for i in range(len(tokenLst))]
    entities = nltk.chunk.ne_chunk(tagged)
    return entities


if __name__ == '__main__':
    #    txt = "John Herbert Dillinger, Jr. (June 22, 1903 2013 July 22, 1934) was an American bank robber in the Depression-era United States. His gang robbed two dozen banks and four police stations. Dillinger escaped from jail twice. Dillinger was also charged with, but never convicted of, the murder of an East Chicago, Indiana, police officer during a shoot-out, Dillinger's only homicide charge."
    #    docToks = preprocText(txt, stemmingOption=False, rmStopwordsOption=False)
    #    for sentTok in docToks:
    #        print sentTok
    #        print postag(sentTok)
    #        print postagCoarseTag(sentTok)
    sentence = "At eight o'clock on Thursday morning Arthur didn't feel very good."
    tokenLst = wordTokenize(sentence)
    entites = nerChunk(tokenLst)

    pass

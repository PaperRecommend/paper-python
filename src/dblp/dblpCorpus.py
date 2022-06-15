import pymongo

import sys
# import toolkit.utility
import re
import os
import json
from tqdm import tqdm


# from dblp.variables import DATA_FOLDER


class Corpus(object):
    docs = None
    numDocs = None
    metaDataFilePath = None
    citFilePath = None
    citeMetaGraph = None
    mongoColl = None

    def __init__(self):
        self.docs = {}
        self.numDocs = {}
        self.citeMetaGraph = {}

        mongoClient = pymongo.MongoClient('mongodb://172.29.7.234:27017/')

        db = mongoClient.admin
        db.authenticate('mongo', 'zrgwmx', mechanism='SCRAM-SHA-1')

        mongoDb = mongoClient['paper']
        self.mongoColl = mongoDb['dblp2']
        self.readMetaDataCitation()

    def readMetaDataCitation(self):
        # metaDict[Eid] = {'Eid': Eid, 'title': title,'year': year,'abstract': abstract}
        metaDict, citeMetaGraph, citDict = self.mongo_metadata_citation()
        cnt = 0
        for Eid in metaDict:
            if Eid not in self.docs:
                self.docs[Eid] = {}
            self.docs[Eid].update(metaDict[Eid])
            cnt += 1
        self.numDocs = len(self.docs)
        print('MetaData {0} entries (#Eid)'.format(cnt))
        self.citeMetaGraph = citeMetaGraph
        cnt = 0
        for Eid in citDict:
            if Eid in self.docs:
                self.docs[Eid]['citLst'] = citDict[Eid]
                cnt += 1
        print('citations {0} entries (#citing paper)'.format(cnt))
        print('citing docs {0} (#edges)'.format(len(self.citeMetaGraph)))
        return

    '''
    1. self.docs[Eid]添加citLst字段, citLst: 列表
       citLst: [{'citingDocEid':citingDocEid, 'citedDocEid':citedDocEid, 'coCitedDocEidLst':coCitedDocEidLst, 'txt':txt},]
    2. citDict: {'Eid': {'citingDocEid':citingDocEid, 'citedDocEid':citedDocEid, 'coCitedDocEidLst':coCitedDocEidLst, 'txt':txt}}
       其中, citingDocEid+coCitedDocEidLst内的所有文章都引用了id为citedDocEid的文章
       其中, Eid和citingDocEid一样, 即引用的文章id
    3. self.citeMetaGraph={}
       citeMetaGraph={}
       citeMetaGraph[citingDocEid]={'citedDocEid':count}
    '''

    # def readCitation(self):
    #     citeMetaGraph, citDict = self.mongo_citation()
    #
    #     # reportCiteMetaGraph(self.citeMetaGraph)
    #     return
    # def mongo_citation(self):
    #     citDict = {}
    #     citeMetaGraph = {}
    #     docscount=0
    #     for item in mongoColl.find({'mag_field_of_study':{'$in':[self.theme]}}):
    #         docscount=docscount+1
    #         if item['has_inbound_citations']:
    #             citedDocEid = item['_id']
    #             for citingDocEid in item['inbound_citations']:
    #                 coCitedDocEidLst = item['inbound_citations'][:].remove(citingDocEid)
    #                 if citingDocEid not in citDict:
    #                     citDict[citingDocEid] = []
    #                 citDict[citingDocEid].append(
    #                     {'citingDocEid': citingDocEid, 'citedDocEid': citedDocEid, 'coCitedDocEidLst': coCitedDocEidLst})
    #                 if citingDocEid not in citeMetaGraph:
    #                     citeMetaGraph[citingDocEid] = {}
    #                 if citedDocEid not in citeMetaGraph[citingDocEid]:
    #                     citeMetaGraph[citingDocEid][citedDocEid] = 0
    #                 citeMetaGraph[citingDocEid][citedDocEid] += 1
    #     self.numDocs=docscount
    #     return citeMetaGraph,citDict

    def mongo_metadata_citation(self):

        metaDict = {}
        for item in self.mongoColl.find({}):
            journal = ""
            if "venue" in item.keys():
                journal = item["venue"]["raw"]
            metaDict[item['_id']] = {
                "Eid": item['_id'],
                "title": item['title'],
                "year": item["year"],
                "journal": journal,
                "fos": [field['name'] for field in item['fos']]
            }
        citations = {}
        with open('../../data/dblp/dblp3.json', 'r', encoding='utf8') as fp2:
            citations = json.load(fp2)

        citDict = {}
        citeMetaGraph = {}

        for Eid in tqdm(citations):
            # metadata
            iEid = int(Eid)
            citing_citations = citations[Eid]
            # citation
            citedDocEid = iEid
            for citingDocEid in citing_citations:

                coCitedDocEidLst = citing_citations[:]
                coCitedDocEidLst.remove(citingDocEid)
                if citingDocEid not in citDict:
                    citDict[citingDocEid] = []
                citDict[citingDocEid].append(
                    {'citingDocEid': citingDocEid, 'citedDocEid': citedDocEid,
                     'coCitedDocEidLst': coCitedDocEidLst})
                if citingDocEid not in citeMetaGraph:
                    citeMetaGraph[citingDocEid] = {}
                if citedDocEid not in citeMetaGraph[citingDocEid]:
                    citeMetaGraph[citingDocEid][citedDocEid] = 0
                citeMetaGraph[citingDocEid][citedDocEid] += 1
        return metaDict, citeMetaGraph, citDict


# ===============================================================================
# Finance Utility
# ===============================================================================
#  从多行文本匹配 id = {...}  中的内容
# EidReg = re.compile('id = (.*?)', re.MULTILINE)
# authorReg = re.compile('author = (.*?)', re.MULTILINE)
# titleReg = re.compile('title = (.*?)', re.MULTILINE)
# yearReg = re.compile('year = (.*?)', re.MULTILINE)
# abstractReg = re.compile('abstract = (.*?)', re.MULTILINE)


def reportCiteMetaGraph(citeMetaGraph):
    # citingDocHist = {}
    # citingCntHist = {}
    # for citingDocId in citeMetaGraph:
    #     citingDoc = len(citeMetaGraph[citingDocId])
    #     citingCnt = sum(citeMetaGraph[citingDocId].values())
    #     if citingDoc not in citingDocHist: citingDocHist[citingDoc] = 0
    #     if citingCnt not in citingCntHist: citingCntHist[citingCnt] = 0
    #     citingDocHist[citingDoc] += 1
    #     citingCntHist[citingCnt] += 1
    # m = max(max(citingDocHist.keys()), max(citingCntHist.keys()))
    # print('[Finance Citing Meta Graph]: report:')
    # print('                          : {0:<20}{1:<20}{2:<20}'.format('i', 'citingDocHist[i]', 'citingCntHist[i]'))
    # for i in range(m):
    #     if (i in citingDocHist) or (i in citingCntHist):
    #         print('                          : {0:<20}{1:<20}{2:<20}'.format(i, citingDocHist.get(i, 0),
    #                                                                          citingCntHist.get(i, 0)))
    # return
    for i in citeMetaGraph.keys():
        print('i:', i, 'citing:', citeMetaGraph[i])


'''
# Citation-based
为每篇文章设置独一无二的id
'''


def getCitMetaGraphEidIdMapping(ed):
    EidToId = {}
    idToEid = {}
    id = 0
    for citingDocEid in ed.citeMetaGraph:
        if citingDocEid not in EidToId:
            EidToId[citingDocEid] = id
            idToEid[id] = citingDocEid
            id += 1
        for citedDocEid in ed.citeMetaGraph[citingDocEid]:
            if citedDocEid not in EidToId:
                EidToId[citedDocEid] = id
                idToEid[id] = citedDocEid
                id += 1
    # with open('EidtoId.json', 'w', encoding='utf8') as fd:
    #     json.dump(EidToId, fd)
    # with open('IdtoEid.json', 'w', encoding='utf8') as fs:
    #     json.dump(idToEid, fs)
    return EidToId, idToEid


'''
    将citeMetaGraph的eid改成id, 并将其改为元组
    data: [(EidToId[citingDocEid], EidToId[citedDocEid], count),]
'''


def getCitMetaGraphDocWrdCntTupleLst(ed, EidToId, idToEid):
    data = []
    for citingDocEid in ed.citeMetaGraph:
        for citedDocEid in ed.citeMetaGraph[citingDocEid]:
            data.append(
                (EidToId[citingDocEid], EidToId[citedDocEid], ed.citeMetaGraph[citingDocEid][citedDocEid]))
    return data


# ===============================================================================
# Content-based
# ===============================================================================
def getContentFreqWrdCntDict(ed, contentField='abstract'):
    freqWrdCntDict = {}
    for Eid in ed.docs:
        toks = (ed.docs[Eid][contentField]).split()
        for tok in toks: freqWrdCntDict[tok] = freqWrdCntDict.get(tok, 0) + 1
    return freqWrdCntDict


def getContentTokIdMapping(ed, freqWrdCntDict=None, threshold=None, contentField='abstract'):
    tokToId = {}
    idToTok = {}
    for Eid in ed.docs:
        toks = (ed.docs[Eid][contentField]).split()
        for tok in toks:
            if tok not in tokToId:
                if (freqWrdCntDict is not None) and (freqWrdCntDict[tok] < threshold): continue
                id = len(tokToId)
                tokToId[tok] = id
                idToTok[id] = tok
    return tokToId, idToTok


def getContentEidIdMapping(ed):
    EidToId = {}
    idToEid = {}
    for Eid in ed.docs:
        if Eid not in EidToId:
            id = len(EidToId)
            EidToId[Eid] = id
            idToEid[id] = Eid
    return EidToId, idToEid


def getContentDocWrdCntTupleLst(ed, tokToId, idToTok, EidToId, idToEid, freqWrdCntDict=None, threshold=None,
                                contentField='abstract'):
    data = []
    for Eid in ed.docs:
        doc = EidToId[Eid]
        wrdCntDict = {}
        for tok in (ed.docs[Eid][contentField]).split():
            if (freqWrdCntDict is not None) and (freqWrdCntDict[tok] < threshold): continue
            wrdCntDict[tokToId[tok]] = wrdCntDict.get(tokToId[tok], 0) + 1
        for wrd in wrdCntDict: data.append((doc, wrd, wrdCntDict[wrd]))
    return data

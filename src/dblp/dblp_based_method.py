'''
Created on Feb 5, 2013

@author: xwang95
'''
import utility
# from paper.paper_lda import PaperLDA
import paper_lda as paper_lda
import os.path
import text
import sys
from variables import DATA_FOLDER
import dblpCorpus as corpus
    # getPubMedCorpus, getCitMetaGraphPmidIdMapping, getCitMetaGraphDocWrdCntTupleLst
import re

import platform

if platform.system().lower() == 'windows':
    RESULT = 'D:\\Custom_Files\\大四上\\毕业设计\\参考项目\\citation_lda\\data\\dblp'
elif platform.system().lower() == 'linux':
    RESULT = '/home/zhengronggui/test/citation_lda/data/dblp'




def getCitationMatrix(ldaInstance):
    print('[citation matrix]: computing citation matrix')
    thetaMatrix = ldaInstance.thetaEstimate
    phiMatrix = ldaInstance.phiEstimate
    topicWeightVec = ldaInstance.topWeiEstimate
    citMatrix = [[0.0 for k1 in range(ldaInstance.K)] for k2 in range(ldaInstance.K)]
    # ===========================================================================
    # P(c2 | c1) = sum_d phi_c1(d) theta_d(c2)
    # ===========================================================================
    cnt = 0
    for d in range(ldaInstance.D):
        k1Lst = [k for k in range(ldaInstance.K) if phiMatrix[k][d] != 0.0]
        k2Lst = [k for k in range(ldaInstance.K) if thetaMatrix[d][k] != 0.0]
        for k1 in k1Lst:
            for k2 in k2Lst:
                citMatrix[k1][k2] += phiMatrix[k1][d] * thetaMatrix[d][k2]
        if (d % 10 == 0): utility.printProgressBar(float(d) / ldaInstance.D)
    print('')
    return citMatrix


def filterTokLst(tokLst): return [tok for tok in tokLst if (len(tok) > 1)]


def getTopicSummary(pmd, pmidToId, idToPmid, ldaInstance, topDocCnt=10, topTokCnt=10, topjournalCnt=5):
    phiMatrix = ldaInstance.phiEstimate
    topWeiVec = ldaInstance.topWeiEstimate
    topicSummary = {}
    for k in range(ldaInstance.K):
        sys.stdout.write('\r[topic summary]: process topic {0}'.format(k))
        sys.stdout.flush()
        tokExptFreq = {}
        journalDist = {}
        yearDist = {}
        topDocs = []
        topToks = []
        for d in range(ldaInstance.D):
            prob = phiMatrix[k][d]
            pmid = idToPmid[d]
            title = pmd.docs[pmid]['title']
            journal = pmd.docs[pmid]['journal']
            year = pmd.docs[pmid]['year']
            fos=pmd.docs[pmid]['fos']
            # 抽取关键词
            tokLst = filterTokLst(text.wordTokenize(title, rmStopwordsOption=True))
            temp=[]
            for field in fos:
                temp.extend(text.wordTokenize(field))
            tokLst.extend(temp)

            for tok in tokLst:
                tokExptFreq[tok] = tokExptFreq.get(tok, 0.0) + prob
            # 出现频率
            journalDist[journal] = journalDist.get(journal, 0.0) + prob
            yearDist[year] = yearDist.get(year, 0.0) + prob
        topDocId = [d for d in sorted(range(ldaInstance.D), key=lambda x: phiMatrix[k][x], reverse=True)][0:topDocCnt]
        topDocs = [(phiMatrix[k][d], pmd.docs[idToPmid[d]]['journal'], pmd.docs[idToPmid[d]]['title'],idToPmid[d]) for d in topDocId]
        topTokId = [t for t in sorted(tokExptFreq, key=lambda x: tokExptFreq[x], reverse=True)][0:topTokCnt]
        topToks = [(tokExptFreq[tok], tok) for tok in topTokId]
        topjournalId = [journal for journal in sorted(journalDist, key=lambda x: journalDist[x], reverse=True)][0:topjournalCnt]
        topjournals = [(journalDist[journal], journal) for journal in topjournalId]
        topicSummary[k] = (topToks, journalDist, yearDist, topWeiVec[k], topDocs, topjournals)
    print('')
    return topicSummary


def dumpTopicSummary(topicSummary, dumpFilePath):
    print('[topic summary]: dump to file {0}'.format(dumpFilePath))
    dumpFile = open(dumpFilePath, 'w')
    for k in sorted(topicSummary, key=lambda k: topicSummary[k][3], reverse=True):
        sys.stdout.write('\r[topic summary]: dump topic {0}'.format(k))
        sys.stdout.flush()
        (topToks, journalDist, yearDist, topWei, topDocs, topjournals) = topicSummary[k]
        dumpFile.write('[Topic: {0}]:{1:.6f}  year={2:.6f}({3:.6f})\n'.format(k, topWei,
                                                                              utility.getDistExpectation(
                                                                                  yearDist),
                                                                              utility.getDistStd(yearDist)))
        for topDoc in topDocs:
            if topDoc is not None and topDoc[0] is not None and topDoc[1] is not None and topDoc[2] is not None:
                dumpFile.write('Doc:{0:.6f}:[{1:^20}]:{2}\n'.format(topDoc[0], topDoc[1], topDoc[2]))
        for topTok in topToks:
            print(topTok)
            if topTok is not None and topTok[0] is not None and topTok[1] is not None:
                dumpFile.write('Tok:{0:.6f}:{1}\n'.format(topTok[0], topTok[1]))
        for topjournal in topjournals:
            if topjournal is not None and topjournal[0] is not None and topjournal[1] is not None:
                dumpFile.write('Ven:{0:.6f}:{1}\n'.format(topjournal[0], topjournal[1]))
        dumpFile.write('\n')
    print('')
    dumpFile.close()
def mongoTopicSummary(topicSummary):
    import pymongo
    mongoClient = pymongo.MongoClient('mongodb://172.29.7.234:27017/')

    db = mongoClient.admin
    db.authenticate('mongo', 'zrgwmx', mechanism='SCRAM-SHA-1')

    mongoDb = mongoClient['paper']
    mongoColl3 = mongoDb['topics']
    for k in sorted(topicSummary, key=lambda k: topicSummary[k][3], reverse=True):
        (topToks, journalDist, yearDist, topWei, topDocs, topjournals) = topicSummary[k]
        item={}
        topicId=k
        year=round(utility.getDistExpectation(yearDist))
        doc=[]
        for topDoc in topDocs:
            if topDoc is not None and topDoc[0] is not None and topDoc[1] is not None and topDoc[2] is not None:
                docWei=topDoc[0]
                docConference=topDoc[1]
                docTitle=topDoc[2]
                docId=topDoc[3]
                doc.append({
                    'id':docId,
                    'title':docTitle,
                    'conference':docConference,
                    'w':docWei
                })
        tok=[]
        for topTok in topToks:
            if topTok is not None and topTok[0] is not None and topTok[1] is not None:
                tokWei=topTok[0]
                tokName=topTok[1]
                tok.append({
                    'name':tokName,
                    'w':tokWei
                })
        conference=[]
        for topjournal in topjournals:
            if topjournal is not None and topjournal[0] is not None and topjournal[1] is not None:
                journalWei=topjournal[0]
                journalName=topjournal[1]
                conference.append({
                    'name':journalName,
                    'w':journalWei
                })

        mongoColl3.insert_one({
            '_id':topicId,
            'w':topWei,
            'year':year,
            'doc':doc,
            'term':tok,
            'conference':conference
        })


def dumpShortTopicSummary(topicSummary, dumpFilePath):
    print('[topic short summary]: dump to file {0}'.format(dumpFilePath))
    dumpFile = open(dumpFilePath, 'w')
    for k in sorted(topicSummary, key=lambda k: topicSummary[k][3], reverse=True):
        sys.stdout.write('\r[topic short summary]: dump topic {0}'.format(k))
        sys.stdout.flush()
        (topToks, journalDist, yearDist, topWei, topDocs, topjournals) = topicSummary[k]
        dumpFile.write('[Topic: {0}]:{1:.6f}  year={2:.2f}({3:.2f}) '.format(k, topWei,
                                                                             utility.getDistExpectation(
                                                                                 yearDist),
                                                                             utility.getDistStd(yearDist)))
        for topTok in topToks: dumpFile.write('{0} '.format(topTok[1]))
        dumpFile.write('\n')
    print('')
    dumpFile.close()


def readTopicSummary(topicSummaryFilePath):
    topicLnRe = re.compile(r'\[Topic: (.*?)\]:(.*?)  year=(.*?)\((.*?)\)')
    docLnRe = re.compile(r'Doc:(.*?):[(.*?)]:(.*)')
    tokLnRe = re.compile(r'Tok:(.*?):(.*)')
    venLnRe = re.compile(r'Ven:(.*?):(.*)')
    topicSummaryDict = {}
    topicSummaryFile = open(topicSummaryFilePath, 'r')
    eof = False
    while (not eof):
        (chunkLnLst, eof) = utility.readChunk(topicSummaryFile)
        topicId = None
        topicProb = None
        topicYearMean = None
        topicYearVar = None
        topDocs = []
        topToks = []
        topVens = []
        for ln in chunkLnLst:
            topicLnReMatch = topicLnRe.match(ln)
            if (topicLnReMatch):
                topicId = utility.parseNumVal(topicLnReMatch.group(1))
                topicProb = utility.parseNumVal(topicLnReMatch.group(2))
                topicYearMean = utility.parseNumVal(topicLnReMatch.group(3))
                topicYearVar = utility.parseNumVal(topicLnReMatch.group(4))
                continue
            docLnReMatch = docLnRe.match(ln)
            if (docLnReMatch):
                docProb = utility.parseNumVal(docLnReMatch.group(1))
                docVen = docLnReMatch.group(2).strip()
                docTitle = docLnReMatch.group(3).strip()
                topDocs.append((docProb, docVen, docTitle))
                continue
            tokLnReMatch = tokLnRe.match(ln)
            if (tokLnReMatch):
                tokProb = utility.parseNumVal(tokLnReMatch.group(1))
                tok = tokLnReMatch.group(2).strip()
                topToks.append((tokProb, tok))
                continue
            venLnReMatch = venLnRe.match(ln)
            if (venLnReMatch):
                venProb = utility.parseNumVal(venLnReMatch.group(1))
                ven = venLnReMatch.group(2).strip()
                topVens.append((venProb, ven))
                continue
        if (topicId is not None): topicSummaryDict[topicId] = (
        topicId, topicProb, topicYearMean, topicYearVar, topDocs, topToks, topVens)
    return topicSummaryDict


def readCitMatrixSummary(citMatrixFilePath):
    citMatrixFile = open(citMatrixFilePath, 'r')
    (citMatrix, eof) = utility.readMatrix(citMatrixFile)
    return citMatrix


NOT_FOLD = True




def CitationLdaSummary(ldaFilePath):
    print('[citation-LDA] loading lda')
    ldaInstance = paper_lda.readLdaEstimateFile(ldaFilePath)
    print('[citation-LDA] loading finance')
    hd = corpus.Corpus()
    print('[citation-LDA] indexing')
    eidToId, idToEid = corpus.getCitMetaGraphEidIdMapping(hd)
    print('[citation-LDA] topic summary generation')
    topicSummary = getTopicSummary(hd, eidToId, idToEid, ldaInstance, topDocCnt=80, topTokCnt=30)
    print('[citation-LDA] topic summary dump')
    dumpTopicSummary(topicSummary, ldaFilePath + '_summary')
    mongoTopicSummary(topicSummary)
    # dumpShortTopicSummary(topicSummary, ldaFilePath + '_shortsummary')
    return


def pubmedCitationLdaShortSummary(ldaFilePath):
    print('[pubmed-citation-LDA]: loading lda')
    #    ldaFilePath = os.path.join(variables.RESULT_DIR, 'pubmed_citation_lda/pubmed_citation_lda_500_145317_145317_0.001_0.001_timeCtrl_10_10.lda')
    ldaInstance = topic_modeling.lda.readLdaEstimateFile(ldaFilePath)
    print('[pubmed-citation-LDA]: loading pubmed')
    pmd = corpus.Corpus()
    print('[pubmed-citation-LDA]: indexing')
    (pmidToId, idToPmid) = corpus.getCitMetaGraphEidIdMapping(pmd)
    print('[pubmed-citation-LDA]: topic summary generation')
    topicSummary = getTopicSummary(pmd, pmidToId, idToPmid, ldaInstance, topDocCnt=10, topTokCnt=20)
    print('[pubmed-citation-LDA]: topic summary dump')
    dumpShortTopicSummary(topicSummary, ldaFilePath + '_shortsummary')
    return


def pubmedCitationPaperSelfCitation(ldaFilePath):
    print('[pubmed-citation-paper-self-citation]: loading lda')
    #    ldaFilePath = os.path.join(variables.RESULT_DIR, 'pubmed_citation_lda/pubmed_citation_lda_500_145317_145317_0.001_0.001_timeCtrl_10_10.lda')
    ldaInstance = paper_lda.readLdaEstimateFile(ldaFilePath)
    print('[pubmed-citation-paper-self-citation]: loading pubmed')
    pmd = corpus.Corpus()
    print('[pubmed-citation-paper-self-citation]: indexing')
    (pmidToId, idToPmid) = corpus.getCitMetaGraphEidIdMapping(pmd)
    paperCitedCntDict = {}
    paperSelfCitationProbDict = {}
    totalCitedCnt = 0
    print('[pubmed-citation-paper-self-citation]: counting cited number')
    for citingPmid in pmd.citeMetaGraph:
        for citedPmid in pmd.citeMetaGraph[citingPmid]:
            paperCitedCntDict[citedPmid] = paperCitedCntDict.get(citedPmid, 0.0) + pmd.citeMetaGraph[citingPmid][
                citedPmid]
            totalCitedCnt += pmd.citeMetaGraph[citingPmid][citedPmid]
    print('[pubmed-citation-paper-self-citation]: counting cited number data_size={0}'.format(totalCitedCnt))
    print('[pubmed-citation-paper-self-citation]: counting self citation prob')
    for pmid in pmidToId: paperSelfCitationProbDict[pmid] = sum([ldaInstance.phiEstimate[k][pmidToId[pmid]] *
                                                                 ldaInstance.thetaEstimate[pmidToId[pmid]][k]
                                                                 for k in range(ldaInstance.K)])
    (paperCitedCntDictRankToKey, paperCitedCntDictKeyToRank) = utility.getDictRank(paperCitedCntDict,
                                                                                           lambda x: paperCitedCntDict[
                                                                                               x], reverse=True)
    (paperSelfCitationProbDictRankToKey, paperSelfCitationProbDictKeyToRank) = utility.getDictRank(
        paperSelfCitationProbDict, lambda x: paperSelfCitationProbDict[x], reverse=True)
    dumpFilePath = ldaFilePath + '_selfCit_probSorted'
    dumpFile = open(dumpFilePath, 'w')
    for r in range(len(paperSelfCitationProbDict)):
        pmid = paperSelfCitationProbDictRankToKey[r]

        rCitedCnt = paperCitedCntDictKeyToRank.get(pmid, 0)
        citedCnt = paperCitedCntDict.get(pmid, -1)

        rSelfCited = paperSelfCitationProbDictKeyToRank[pmid]
        selfCitedProb = paperSelfCitationProbDict[pmid]

        title = pmd.docs[pmid]['title']
        dumpFile.write('CitedCnt= {0:>6}({1:>6})\t SelfCitedProb= {2:.6f}({3:>6}): {4}\n'.format(citedCnt, rCitedCnt,
                                                                                                 selfCitedProb,
                                                                                                 rSelfCited, title))
    dumpFile.close()
    return


def CitationMatrix(ldaFilePath):
    print('[pubmed-citation-LDA]: loading lda')
    ldaInstance = paper_lda.readLdaEstimateFile(ldaFilePath)
    citMatrix = getCitationMatrix(ldaInstance)
    citMatrixFile = open(ldaFilePath + '_citMatrix', 'w')
    citMatrixFile.write('\n'.join([' '.join([str(x) for x in vec]) for vec in citMatrix]))
    citMatrixFile.close()
    return


def pubmedTimeSortedCitationMatrix(citMatrixFilePath, topicSummaryFilePath):
    print('[Time Sorted Citation Matrix]: read citation file: {0}'.format(citMatrixFilePath))
    citMatrix = readCitMatrixSummary(citMatrixFilePath)
    print('[Time Sorted Citation Matrix]: read topic summary file: {0}'.format(topicSummaryFilePath))
    topicSummaryDict = readTopicSummary(topicSummaryFilePath)
    print('[Time Sorted Citation Matrix]: sort topic on time')
    timeSortedTopicIdLst = [topicId for topicId in sorted(topicSummaryDict, key=lambda x: topicSummaryDict[x][2])]
    print('[Time Sorted Citation Matrix]: sort topic on time -- read {0} topics'.format(len(timeSortedTopicIdLst)))
    print('[Time Sorted Citation Matrix]: compute time sorted citation matrix')
    timeSortedCitMatrix = [[citMatrix[k1][k2] for k2 in timeSortedTopicIdLst] for k1 in timeSortedTopicIdLst]
    timeSortedCitMatrixFilePath = citMatrixFilePath + '_timeSorted'
    print(
        '[Time Sorted Citation Matrix]: dump time sorted citation matrix file {0}'.format(timeSortedCitMatrixFilePath))
    timeSortedCitMatrixFile = open(timeSortedCitMatrixFilePath, 'w')
    timeSortedCitMatrixFile.write('\n'.join([' '.join([str(x) for x in vec]) for vec in timeSortedCitMatrix]))
    return timeSortedCitMatrix


def pubmedTimeSortedShortTopicSummary(topicSummaryFilePath):
    print('[Time Sorted Short Summary]: read summary file {0}'.format(topicSummaryFilePath))
    topicSummaryDict = readTopicSummary(topicSummaryFilePath)
    dumpFile = open(topicSummaryFilePath + '_timeSorted_shortSummary', 'w')
    timeSort = 0
    for k in sorted(topicSummaryDict, key=lambda k: topicSummaryDict[k][2]):
        sys.stdout.write('\r[Time Sorted Short Summary]: read topic {0}'.format(k))
        sys.stdout.flush()
        (topicId, topicProb, topicYearMean, topicYearVar, topDocs, topToks, topVens) = topicSummaryDict[k]
        dumpFile.write('[timeSort:{0},\tTopic: {1}]:\t{2:.6f}  year={3:.2f}({4:.2f})\t'.format(timeSort, k, topicProb,
                                                                                               topicYearMean,
                                                                                               topicYearVar))
        for topTok in topToks: dumpFile.write('{0} '.format(topTok[1]))
        dumpFile.write('\n')
        timeSort += 1
    print('')
    dumpFile.close()


if (__name__ == '__main__'):
    # ===========================================================================
    # RUN LDA
    # ===========================================================================
    #    pubmedCitationLdaRun()

    # ===========================================================================
    # READ LDA FILE
    # ===========================================================================
    #    pubmedCitationLdaSummary()

    # ===========================================================================
    # RUN TIME SORTED CITATION MATRIX
    # ===========================================================================
    #    pubmedTimeSortedCitationMatrix('/home/xwang95/result/pubmed_citation_lda/pubmed_citation_lda_100_145317_145317_0.001_0.001_timeCtrl_30_45.lda_citMatrix',
    #                                   '/home/xwang95/result/pubmed_citation_lda/pubmed_citation_lda_100_145317_145317_0.001_0.001_timeCtrl_30_45.lda_summary')
    # ldaFilePath1 = os.path.join(variables.RESULT_DIR, 'pubmed_citation_lda',
    #                             'pubmed_citation_lda_500_145317_145317_0.001_0.001_timeCtrl_10_10.lda')
    # ldaFilePath2 = os.path.join(DATA_FOLDER,THEME,
    #                             'pubmed_citation_lda_100_145317_145317_0.001_0.001_timeCtrl_30_45.lda')
    # pubmedCitationPaperSelfCitation(ldaFilePath2)
    ldaFilePath = os.path.join(DATA_FOLDER,'citation_lda_400_481629_481629_1e-06_1e-06_timeCtrl_0.5_0.5.lda')
    CitationMatrix(ldaFilePath)
    CitationLdaSummary(ldaFilePath)

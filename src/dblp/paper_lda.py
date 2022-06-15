import os
import random
import sys
import time
import utility
import dblpCorpus as corpus
import json
import platform
from variables import DATA_FOLDER


# K: 主题数 D: 文章数 W: 单词数
class PaperLDA(object):
    K = None
    D = None
    W = None
    Alpha = None  # alpha向量
    Beta = None
    AlphaSum = None
    BetaSum = None

    ObsDW = None  # 每篇文档引用的文章数(相当于lda中一篇文章的单词数)
    ObsDWK = None  # 第d篇文档第w篇引文的每个主题个数

    ObsDK = None  # 每一篇文档每个主题的个数
    ObsWK = None  # 第w篇引文的每个主题的个数
    ObsK = None  # 主题个数

    ObsW = None  # 引文个数
    ObsD = None  # 文档个数
    Obs = None

    BurninIter = None
    SampliIter = None

    BurninTime = None
    SampliTime = None

    iterCtrl = None
    iter = None

    thetaEstimate = None
    phiEstimate = None
    topWeiEstimate = None

    def queryDocTopic(self, d, k, exD, exW, exK):
        x = float(self.ObsDK[d].get(k, 0.0))
        if exD == d and exK == k: x -= 1.0
        return x

    def queryWordTopic(self, w, k, exD, exW, exK):
        x = float(self.ObsWK[w].get(k, 0.0))
        if exW == w and exK == k: x -= 1.0
        return x

    def queryTopic(self, k, exD, exW, exK):
        x = float(self.ObsK.get(k, 0.0))
        if exK == k: x -= 1.0
        return x

    # 累积法
    def multinomialSampling(self, pdf):
        x = random.random()
        i = 0
        cdf = 0.0
        while True:
            cdf += pdf[i]
            if cdf >= x: return i
            i += 1
            if i > len(pdf): return len(pdf) - 1
        # return

    '''
        d: citingId, w: citedId, k: random(0,K-1 )
    '''

    def gibbsSamplingUpdateDWK(self, d, w, k):
        # python2和3的区别 3需要加list()否则返回的是map对象，无法迭代
        # alpha+1或alpha+2或alpha,term1: [alpha+1,alpha+1,...]
        # 吉布斯采样公式
        terms1 = list(map(lambda i: self.Alpha[i] + self.queryDocTopic(d, i, d, w, k), range(0, self.K)))

        # beta+1或beta+2或alpha,term1: [beta+1,beta+1,...]
        terms2 = list(map(lambda i: self.Beta[w] + self.queryWordTopic(w, i, d, w, k), range(0, self.K)))

        terms3 = list(map(lambda i: self.BetaSum + self.queryTopic(i, d, w, k), range(0, self.K)))
        propProb = list(map(lambda i: terms1[i] * terms2[i] / terms3[i], range(0, self.K)))
        pdf = list(map(lambda i: propProb[i] / sum(propProb), range(0, self.K)))

        # 累积法
        newK = self.multinomialSampling(pdf)

        self.ObsDWK[d][w][k] -= 1.0
        self.ObsDWK[d][w][newK] = self.ObsDWK[d][w].get(newK, 0.0) + 1.0
        self.ObsDK[d][k] -= 1.0
        self.ObsDK[d][newK] = self.ObsDK[d].get(newK, 0.0) + 1.0
        self.ObsWK[w][k] -= 1.0
        self.ObsWK[w][newK] = self.ObsWK[w].get(newK, 0.0) + 1.0
        self.ObsK[k] -= 1.0
        self.ObsK[newK] = self.ObsK.get(newK, 0.0) + 1.0
        return

    def gibbsSamplingUpdateDW(self, d, w):
        sampQueue = {}
        for k, c in self.ObsDWK[d][w].items(): sampQueue[k] = c
        for k, c in sampQueue.items(): [self.gibbsSamplingUpdateDWK(d, w, k) for t in range(0, int(c))]
        return

    def gibbsSamplingUpdateD(self, d):
        for w in self.ObsDW[d].keys(): self.gibbsSamplingUpdateDW(d, w)
        return

    def iteration(self):
        self.iter += 1
        for d in self.ObsDW.keys(): self.gibbsSamplingUpdateD(d)
        return

    # ===============================================================================
    # sInsert: Initialization
    # ===============================================================================

    '''
        d: citingId, w: citedId , k: random(0,K-1)
    '''

    def sInsertDWK(self, d, w, k):
        if d not in self.ObsDWK: self.ObsDWK[d] = {}
        if w not in self.ObsDWK[d]: self.ObsDWK[d][w] = {}
        self.ObsDWK[d][w][k] = self.ObsDWK[d][w].get(k, 0.0) + 1.0
        return

    def sInsertDK(self, d, k):
        if d not in self.ObsDK: self.ObsDK[d] = {}
        self.ObsDK[d][k] = self.ObsDK[d].get(k, 0.0) + 1.0
        return

    def sInsertWK(self, w, k):
        if w not in self.ObsWK: self.ObsWK[w] = {}
        self.ObsWK[w][k] = self.ObsWK[w].get(k, 0.0) + 1.0
        return

    def sInsertK(self, k):
        self.ObsK[k] = self.ObsK.get(k, 0.0) + 1.0
        return

    '''
    d: citingId, w: citedId , k: random(0,K-1)
    '''

    def sInsert(self, d, w, k):
        self.sInsertDWK(d, w, k)
        self.sInsertDK(d, k)
        self.sInsertWK(w, k)
        self.sInsertK(k)
        return

    '''
    self.ObsDW:{'citingId':{'citedId':count(1)}, }
    self.ObsD: {'citingId':引用的文章数}
    self.ObsW: {'citedId':被引用的文章数}
    self.Obs: 所有文章总引用文章数=所有文章被引用总次数
    '''

    def sInsertDW(self, d, w, c):
        if d not in self.ObsDW: self.ObsDW[d] = {}
        self.ObsDW[d][w] = float(self.ObsDW[d].get(w, 0.0) + c)
        self.ObsD[d] = self.ObsD.get(d, 0.0) + c
        self.ObsW[w] = self.ObsW.get(w, 0.0) + c
        self.Obs += c
        return

    # ===========================================================================
    # update parameter estimation
    # ===========================================================================
    def updateThetaEstimate(self):
        for d in range(self.D):
            for k in range(self.K):
                self.thetaEstimate[d][k] += (self.Alpha[k] + self.ObsDK.get(d, {}).get(k, 0.0)) / (
                        self.AlphaSum + self.ObsD.get(d, 0.0))
        return

    def updatePhiEstimate(self):
        for k in range(self.K):
            for w in range(self.W):
                self.phiEstimate[k][w] += (self.Beta[w] + self.ObsWK.get(w, {}).get(k, 0.0)) / (
                        self.BetaSum + self.ObsK.get(k, 0.0))
        return

    def updateTopWeiEstimate(self):
        for k in self.ObsK.keys(): self.topWeiEstimate[k] += self.ObsK.get(k, 0) / self.Obs
        return

    # ============================================================================
    # initialization parameters
    # ============================================================================
    '''
    data: [(EidToId[citingDocEid], EidToId[citedDocEid], count), ]
    K: 主题数量(50)
    D: 文档数量
    W: word数量, 设置为与文档数量相同, 即D=W
    alpha: 1e-6
    beta: 1e-6
    burnTime: 0.5
    sampTime: 0.5
    iterCtrl: False
    '''

    def __init__(self, data, K, D, W, alpha, beta, burnIter=None, sampIter=None, burnTime=None, sampTime=None,
                 iterCtrl=None):
        print('[LDA]: Initializing and Loading Data')
        print('[LDA]: K = {0}'.format(K))
        print('[LDA]: D = {0}'.format(D))
        print('[LDA]: W = {0}'.format(W))
        print('[LDA]: data size = {0}'.format(len(data)))

        # data: citing cited num
        self.K = K
        self.D = D
        self.W = W
        # 1e-6=0.0024
        # self.Alpha: [1e-6,1e-6,] K(50)个
        # self.Beta: [1e-6,1e-6,] W个
        self.Alpha = [alpha for k in range(self.K)]
        self.Beta = [beta for w in range(self.W)]
        # 50
        self.AlphaSum = sum(self.Alpha)
        # W
        self.BetaSum = sum(self.Beta)

        '''
        self.ObsDW:{'citingId':{'citedId':count(1)}, }
        self.ObsD: {'citingId':引用的文章数}
        self.ObsW: {'citedId':被引用的文章数}
        self.Obs: 所有文章总引用文章数=所有文章被引用总次数
        '''
        self.ObsDW = {}
        self.ObsDWK = {}

        self.ObsDK = {}
        self.ObsWK = {}
        self.ObsK = {}

        self.ObsW = {}
        self.ObsD = {}
        self.Obs = 0.0

        # D*K矩阵 0.0 主题分布
        self.thetaEstimate = [[0.0 for k in range(self.K)] for d in range(self.D)]

        # K*W矩阵 0.0 引文分布
        self.phiEstimate = [[0.0 for w in range(self.W)] for k in range(self.K)]

        # 1*K 主题权重
        self.topWeiEstimate = [0.0 for k in range(self.K)]

        if iterCtrl:
            self.BurninIter = burnIter
            self.SampliIter = sampIter
            self.iterCtrl = True
        else:
            self.BurninTime = burnTime
            self.SampliTime = sampTime
            self.iterCtrl = False

        self.iter = 0

        for v in data:
            d = v[0]  # citing id
            w = v[1]  # cited id
            c = v[2]  # count
            self.sInsertDW(d, w, c)  # initialize obsDW
        return

    # ===========================================================================
    # Markov Chain Monte Carlo
    # ===========================================================================
    def Mcmc(self):
        '''initialization'''
        print('[LDA]: Randomizing Topic Assignment')
        # d: citingId, w: citedId
        for d in self.ObsDW.keys():
            for w in self.ObsDW[d].keys():
                [self.sInsert(d, w, random.randint(0, self.K - 1))
                 for t in range(int(self.ObsDW[d][w]))]

        '''run'''
        if self.iterCtrl:
            print('[LDA]: iteration-controlled')
            # burn-in
            print('[LDA]: Burning-in ... (expected iter {0})'.format(self.BurninIter))
            print('       Progress:')
            prog = 0.0
            mil = 0.0
            step = 0.05
            timeStart = time.clock()
            for t in range(0, int(self.BurninIter)):
                prog = float(t) / self.BurninIter
                utility.printProgressBar(prog, step)
                self.iteration()
            print('')
            timeNow = time.clock()
            hr = ((timeNow - timeStart) / 3600.0)
            print('[LDA]: Burn-in time = {0}hr'.format(hr))
            self.BurninTime = hr
            # sampling
            print('[LDA]: Sampling  ... (expected iter {0})'.format(self.SampliIter))
            print('       Progress:')
            prog = 0.0
            mil = 0.0
            step = 0.05
            timeStart = time.clock()
            for t in range(0, int(self.SampliIter)):
                prog = float(t) / self.SampliIter
                utility.printProgressBar(prog, step)
                self.iteration()
                self.updatePhiEstimate()
                self.updateThetaEstimate()
                self.updateTopWeiEstimate()
            print('')
            timeNow = time.clock()
            hr = ((timeNow - timeStart) / 3600.0)
            print('[LDA]: Sampling time = {0}hr'.format(hr))
            self.SampliTime = hr
        else:
            self.BurninIter = 0
            self.SampliIter = 0
            print('[LDA]: time-controlled')
            # burn-in
            print('[LDA]: Burning-in ... (expected time {0}hr)'.format(self.BurninTime))
            print('       Progress:')
            prog = 0.0
            mil = 0.0
            step = 0.05
            timeStart = time.process_time()
            while (True):
                timeNow = time.process_time()
                prog = ((timeNow - timeStart) / 3600.0) / self.BurninTime
                utility.printProgressBar(prog, step, 'iter = {0}, elapsed_time = {1}'.format(self.BurninIter,
                                                                                             timeNow - timeStart))
                if (prog >= 1.0): break
                self.iteration()
                self.BurninIter += 1
            print('')
            print('[LDA]: Burn-in iterations = {0}'.format(self.BurninIter))
            # sampling
            print('[LDA]: Sampling ... (expected time {0}hr)'.format(self.SampliTime))
            print('       Progress:')
            prog = 0.0
            mil = 0.0
            step = 0.05
            timeStart = time.process_time()
            while True:
                timeNow = time.process_time()
                prog = ((timeNow - timeStart) / 3600.0) / self.SampliTime
                utility.printProgressBar(prog, step, 'iter = {0}, elapsed_time = {1}'.format(self.SampliIter,
                                                                                             timeNow - timeStart))
                if prog >= 1.0:
                    break
                print(prog)
                self.iteration()
                self.updatePhiEstimate()
                self.updateThetaEstimate()
                self.updateTopWeiEstimate()
                self.SampliIter += 1
            print('')
            print('[LDA]: Sampling iterations = {0}'.format(self.SampliIter))
        '''estimation'''
        print('[LDA]: Estimating sample averages')
        # theta: 某篇文章的主题分布 phi: 某个主题的词分布
        self.thetaEstimate = [utility.normalizeVector(vec) for vec in self.thetaEstimate]
        self.phiEstimate = [utility.normalizeVector(vec) for vec in self.phiEstimate]
        self.topWeiEstimate = utility.normalizeVector(self.topWeiEstimate)
        print('[LDA]: End')
        '''end'''
        return (self.thetaEstimate, self.phiEstimate, self.topWeiEstimate)


# ===========================================================================
# DUMP & READ
# ===========================================================================
def dumpLdaEstimateFile(ldaInstance, dumpFilePath):
    # utility.removePath(dumpFilePath)
    print('[LDA-util]: dump file to {0}'.format(dumpFilePath))
    dumpFile = open(dumpFilePath, 'w', encoding='utf-8')
    dumpFile.write('K = {0}\n'.format(ldaInstance.K))
    dumpFile.write('D = {0}\n'.format(ldaInstance.D))
    dumpFile.write('W = {0}\n'.format(ldaInstance.W))
    dumpFile.write('burnin_iter = {0}\n'.format(int(ldaInstance.BurninIter)))
    dumpFile.write('sampli_iter = {0}\n'.format(int(ldaInstance.SampliIter)))
    dumpFile.write('alpha: [1 * K]\n')
    dumpFile.write(' '.join([str(x) for x in ldaInstance.Alpha]) + '\n')
    dumpFile.write('beta: [1 * W]\n')
    dumpFile.write(' '.join([str(x) for x in ldaInstance.Beta]) + '\n')
    dumpFile.write('theta: [D * K]\n')
    dumpFile.write('\n'.join([' '.join([str(x) for x in vec]) for vec in ldaInstance.thetaEstimate]) + '\n')
    dumpFile.write('phi: [K * W]\n')
    dumpFile.write('\n'.join([' '.join([str(x) for x in vec]) for vec in ldaInstance.phiEstimate]) + '\n')
    dumpFile.write('topic_weight: [1 * K]\n')
    dumpFile.write(' '.join([str(x) for x in ldaInstance.topWeiEstimate]))
    dumpFile.close()
    return


def readLdaEstimateFile(dumpFilePath):
    print('[LDA-util]: read file from {0}'.format(dumpFilePath))
    dumpFile = open(dumpFilePath, 'r', encoding='utf-8')
    sys.stdout.write('\r[LDA-util]: read K, D, W                   ')
    sys.stdout.flush()
    K = utility.parseNumVal(utility.rmLeadingStr(dumpFile.readline(), 'K = '))
    D = utility.parseNumVal(utility.rmLeadingStr(dumpFile.readline(), 'D = '))
    W = utility.parseNumVal(utility.rmLeadingStr(dumpFile.readline(), 'W = '))
    sys.stdout.write('\r[LDA-util]: read burninIter, sampliIter    ')
    sys.stdout.flush()
    burninIter = utility.parseNumVal(utility.rmLeadingStr(dumpFile.readline(), 'burnin_iter = '))
    sampliIter = utility.parseNumVal(utility.rmLeadingStr(dumpFile.readline(), 'sampli_iter = '))
    sys.stdout.write('\r[LDA-util]: read alpha, beta               ')
    sys.stdout.flush()
    dumpFile.readline()
    (alpha, eof) = utility.readVector(dumpFile)
    dumpFile.readline()
    (beta, eof) = utility.readVector(dumpFile)
    sys.stdout.write('\r[LDA-util]: read {theta}                   ')
    sys.stdout.flush()
    dumpFile.readline()
    (theta, eof) = utility.readMatrix(dumpFile, D)
    sys.stdout.write('\r[LDA-util]: read {phi}                     ')
    sys.stdout.flush()
    dumpFile.readline()
    (phi, eof) = utility.readMatrix(dumpFile, K)
    sys.stdout.write('\r[LDA-util]: read topicWeight               ')
    sys.stdout.flush()
    dumpFile.readline()
    (topicWeight, eof) = utility.readVector(dumpFile)
    sys.stdout.write('\r[LDA-util]: read complete                  ')
    sys.stdout.flush()
    print('')
    ldaInstance = PaperLDA([], K, D, W, 0, 0, burninIter, sampliIter, iterCtrl=True)
    ldaInstance.thetaEstimate = theta
    ldaInstance.phiEstimate = phi
    ldaInstance.topWeiEstimate = topicWeight
    dumpFile.close()
    return ldaInstance


# 不同主题需修改文件前缀
'''
data: [(EidToId[citingDocEid], EidToId[citedDocEid], count), ]
K: 主题数量(50)
D: 文档数量
W: word数量, 设置为与文档数量相同, 即D=W
alpha: 1e-6
beta: 1e-6
burninTimeHr: 0.5
sampliTimeHr: 0.5
dumpFileFolder: 存放生成文件的文件夹路径
'''


def citationLdaRun(data, K, D, W, alpha, beta, burninTimeHr, sampliTimeHr):
    dumpFilePath = os.path.join(DATA_FOLDER,
                                'citation_lda_{0}_{1}_{2}_{3}_{4}_{5}_{6}_{7}.lda'.format(K, D, W, alpha, beta,
                                                                                          'timeCtrl',
                                                                                          burninTimeHr,
                                                                                          sampliTimeHr))
    # utility.removePath(dumpFilePath)
    # lda对象
    ldaInstance = PaperLDA(data, K, D, W, alpha, beta, burnTime=burninTimeHr, sampTime=sampliTimeHr, iterCtrl=False)
    # 训练生成theta, phi, topicweights
    # theta~Dir(alpha) 主题分布
    # phi~Dir(beta) 词分布
    # topicWeights:
    (postTheta, postPhi, topicWeights) = ldaInstance.Mcmc()

    # 把结果写入文件
    dumpLdaEstimateFile(ldaInstance, dumpFilePath)
    return postTheta, postPhi, topicWeights


'''
# ec: Theme对象
data: [(EidToId[citingDocEid], EidToId[citedDocEid], count), ]
'''


def CitationLdaRun(K, BurninHr, SampliHr):
    # 存放.lda文件文件夹

    print('[citation-LDA]: loading')

    # 从两个文件中获得finance语料, ec是Finance对象

    ec = corpus.Corpus()
    print('[citation-LDA]: indexing')

    # 为每篇文章设置独一无二的id eid:id, id:eid
    eidToId, idToEid = corpus.getCitMetaGraphEidIdMapping(ec)

    # D: 所有文章数量
    D = len(eidToId)
    print('                       size: {0}'.format(D))
    print('[citation-LDA]: insert tuple (doc, wrd, cnt) to list')
    # data: [(EidToId[citingDocEid], EidToId[citedDocEid], count), ]
    data = corpus.getCitMetaGraphDocWrdCntTupleLst(ec, eidToId, idToEid)
    print('                       size: {0}'.format(len(data)))
    '''running LDA'''
    (postTheta, postPhi, topicWeights) = citationLdaRun(data, K, D, D, 1e-6, 1e-6, BurninHr, SampliHr)


if __name__ == '__main__':
    # 主要计算 theta phi topweight
    # themes=['Biology','Business','Chemistry','Economics','Computer Science','Engineering','Geography','Geology','Material Science','Medicine','']
    CitationLdaRun(400, 0.5, 0.5)

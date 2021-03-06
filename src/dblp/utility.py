import random
import os
import os.path
import sys
import shutil
import math

NOT_FOLD = True
#===============================================================================
# Section: Topic Modeling
#===============================================================================
def getRankedIdxList(lst, reverseOption=False):
    return [idx for idx in sorted(range(len(lst)), key=lambda x: lst[x], reverse=reverseOption)]

def getRankedIdxMatrix(mtx, reverseOption=False):
    return [getRankedIdxList(lst, reverseOption) for lst in mtx]

def getDictRank(dict, key, reverse=False):
    rankToKey = {}
    keyToRank = {}
    for k in sorted(dict, key=key, reverse=reverse):
        r = len(rankToKey)
        rankToKey[r] = k
        keyToRank[k] = r
    return (rankToKey, keyToRank)

NOT_FOLD = True
#===============================================================================
# parse
#===============================================================================
def parseNumVal(s):
    s = s.strip()
    try:
        v = int(s)
    except:
        try:
            v = float(s)
        except:
            v = 0
    return v

def parseYear(s):
    yearLst = []
    cnt = 0
    for i in range(len(s)):
        if('0' <= s[i] and s[i] <= '9'): cnt += 1
        else: cnt = 0
        if(cnt == 4): yearLst.append(parseNumVal(s[(i - 3):(i + 1)]))
    return yearLst

NOT_FOLD = True
#===============================================================================
# I/O
#===============================================================================
def readLines(num, reader):
    eof = False
    lineLst = [reader.readline() for i in range(num)]
    if(not lineLst[num - 1]): eof = True
    lineLst = [line.strip('\n') for line in lineLst]
    # reader.readline()
    return (lineLst, eof)

def readUntil(checkFunc, reader):
    eof = False
    lineLst = []
    while(True):
        lineLst.append(reader.readline())
        if(not lineLst[-1]):
            eof = True
            break
        if(checkFunc(lineLst[-1].strip())): break
    lineLst = [line.strip() for line in lineLst]
    if(eof): lineLst = lineLst[:-1]  # no end of file line
    return (lineLst, eof)

def readChunk(reader): return readUntil(lambda x: (x == ""), reader)

def readMatrix(reader, row=None):
    lines = []
    if(row is not None): (lines, eof) = readLines(row, reader)
    else: (lines, eof) = readUntil(lambda x: (x == ''), reader)
    return ([[parseNumVal(s) for s in line.split()] for line in lines], eof)

def readVector(reader):
    (lines, eof) = readLines(1, reader)
    if(not eof): return ([parseNumVal(s) for s in lines[0].split()], eof)
    else: return([], eof)

def printProgressBar(prog, step=0.04, addStr=""):
    prct = int(math.ceil(100 * prog))
    totlBlk = int(math.ceil(1.0 / step))
    progBlk = max(min(totlBlk, int(math.ceil(prog / step))) - 1, 0)
    futrBlk = max(totlBlk - progBlk - 1, 0)
    bar = '[' + ''.join(['=' for i in range(progBlk)]) + '>' + ''.join([' ' for i in range(futrBlk)]) + ']' + '({0})'.format(addStr)
    s = '\r[{0:>3}%]: {1}'.format(prct, bar)
    sys.stdout.write(s)
    sys.stdout.flush()
    return s

def rFillSpaces(s, lineWidth=300):
    return str(s) + ''.join([' ' for i in range(lineWidth - len(s))])

NOT_FOLD = True
#===============================================================================
# string
#===============================================================================
def rmLeadingStr(s, srm):
    if s == srm:
        return ''
    if s.startswith(srm): 
        return s[len(srm):]
    return s

def rmTrailingStr(s, srm):
    if(s.endswith(srm)): return s[:-len(srm)]


#===============================================================================
# file management
#===============================================================================
def removePath(path):
    if(not os.path.exists(path)): print('[remove_path@utility]: path not exist: {0}, Doing Nothing'.format(path))
    if(os.path.isdir(path)):
        print('[remove_path@utility]: removing directory: {0}'.format(path))
        shutil.rmtree(path)
    if(os.path.isfile(path)):
        print('removing files: {0}'.format(path))
        os.remove(path)
    return

def mkDir(path):
    if(os.path.isdir(path)): print('[make_dir@utility]: path already exists as DIRECTORY! Doing Nothing')
    elif(os.path.isfile(path)): print('[make_dir@utility]: path already exist as FILE! Doing Nothing')
    else:
        print('[make_dir@utility]: make directory {0}'.format(path))
        os.makedirs(path)
    return

NOT_FOLD = True
#===============================================================================
# computing
#===============================================================================
def getVecNorm(vec, order): return math.pow(sum([math.pow(x, order) for x in vec]), 1.0 / order)

def normalizeVector(vec, order=None):
    if(order is not None): norm = getVecNorm(vec, order)
    else: norm = float(sum(vec))
    return [x / norm for x in vec]

def getDistExpectation(dist):
    if dist is None:
        return 0
    sum=0
    for k in dist:
        if dist[k] is None or k is None:
            continue
        else:
            sum=sum+k * dist[k]
    return sum

def getDistVariation(dist, expt=None):
    if(expt is None): expt = getDistExpectation(dist)
    sum=0
    for k in dist:
        if k is None or dist[k] is None:
            continue
        else:
            sum=sum+ ((k - expt) ** 2) * dist[k]
    return sum

def getDistStd(dist, expt=None): return math.sqrt(getDistVariation(dist, expt))

def getMatrixVecMultiply(m, v): return [sum([vec[i] * v[i] for i in range(len(v))]) for vec in m]

def getTransposeSquareMatrix(m): return [[m[j][i] for j in range(len(m))] for i in range(len(m))]

def getVecSubstract(v1, v2): return [v1[i] - v2[i] for i in range(len(v1))]

if __name__ == '__main__':
    pass

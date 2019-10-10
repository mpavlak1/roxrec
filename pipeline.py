#/roxrec/pipeline.py
'''
## ========================================================================== ##
- Pipeline class used to store information about client MongoDB connection,
   and standardizes preprocessing and hashing functions that need to be
   consistent across all other classes.
- Pipeline provides additional functions to preform standard operations
  on MongoDB, such as generating collection indices by relevant fields.
## Author: Michael Pavlak
## ========================================================================== ##
'''

## Built-ins
from itertools import combinations

## Local
import __init__
from general import *

## Additioanl
import pymongo
import pymongo.errors
from nltk import ngrams


class Pipeline():
    '''Co-ordination class to store meta data for pipe line static functions and connection to external data sources'''

    def __connect__(host='localhost',port=27017):
        return pymongo.MongoClient('mongodb://{}:{}'.format(host,port))

    __database__ = 'temp'
    __validchars__ = [9,10,32] + list(range(48,57+1)) + list(range(65,90+1)) + list(range(97,122+1))
    __replacementtable__ = {}
    for i in range(0,1<<7):   
        if(i not in __validchars__): __replacementtable__[i] = None
        else: __replacementtable__[i] = i if i not in range(97,122+1) else i^32

    def __init__(self, table, client = None, database = __database__, replacementtable = None):

        self.database = database
        self.table = table
        self.__client__ = client

        self.__replacementtable__ = replacementtable if replacementtable else Pipeline.__replacementtable__

    def clone(self):
        try:
            self.client.close()
        except Exception: pass
        return Pipeline(self.table,None,self.database,self.__replacementtable__)

    def client(self):
        if(self.__client__): return self.__client__
        else:
            self.connect()
            return self.__client__

    def connect(self):
        if(not self.__client__):
            self.__client__ = Pipeline.__connect__()

    def clean(self, s):
        return s.translate(self.__replacementtable__).strip()

    def hashrec(self, record):
        return sum(int.from_bytes(h32(b).digest(),byteorder='little') for b in map(str,record.values()))

    def get_tri_grams(self,word):
        L = len(word)
        if(L == 1):
            yield ''.join((word[0], word[0], word[0]))
        elif(L == 2):
            yield ''.join((word[0], word[0], word[1]))
            yield ''.join((word[0], word[1], word[1]))
        else:
            for gram in set(ngrams(word, 3)):
                yield ''.join(gram)

    def build_index(self, table, fields):
        e_idx = list(map(lambda x: x['key'], self.client()[self.database][table].index_information().values()))
        for i in range(1, len(fields)):
            for combo in combinations(fields, i):
                idx = []
                for c in combo:
                    idx.append((c, pymongo.ASCENDING))
                if(idx not in e_idx): self.client()[self.database][table].create_index(idx)   

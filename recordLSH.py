#/roxrec/recordLSH.py
'''
## ========================================================================== ##
- RecordLSH uses LSH to filter potential canidate records with highest
   probability of having a high match rate when stronger similarity
   metric applied.
- RecordLSH generates hash families for each fuzzy field in record
- Canidate match pool drawn from collection of canidates for each individual
   field.
- Bag of Words and Cosine Similarity used to determine the 'distance' between
   records. Bag of Words will use a next highest discounted word
   (smallest edit distance) when counting the appearence of each word
   in the combine word set. This results in slightly better accuracy and less
   varability than if exact match required (or stemmed match).
- Filter field used to limit the records in hash family to those who fields
   exactly match the filter
- Field Weights used to give varabile significance to different fields. When
   field weights not provided, all fields in the fileds attribute given equal
   weight of one. Field weight not taken into consideration when pooling from
   individual field LSH functions.
## Author: Michael Pavlak
## ========================================================================== ##
'''

##Built-ins
import itertools

## Package
import __init__
from general import *
from broker import Broker
from pipeline import Pipeline
from preprocesspiper import PreprocessPiper

## Additional
from sortedcontainers import SortedList
from datasketch import MinHash, MinHashLSH

def __LSHName__(fields, filter_):
    name = 'RecordLSH'
    filter_tags = []
    for x in filter_:
        filter_tags.append('{}={}'.format(x,filter_[x]))
    if(len(filter_)==0):
        return '{name}_{}'.format('_'.join(fields), name=name)
    return '{name}_{}_by_{}'.format('_'.join(fields), '_'.join(filter_tags) if len(filter_tags) > 0 else '', name = name)
    
def RecordLSHFactory(pipeline, fields, filter_={}, field_weights = None):

    name = __LSHName__(fields, filter_)
    broker = Broker(pipeline)
    try:
        i = broker.download_obj(name)
        i.LSH = {}
        for key in i.fields:
            i.LSH[key] = broker.download_obj('{}_{}'.format(name,key))
        assert isinstance(i, RecordLSH)
        return i
    except AssertionError:
        return RecordLSH(pipeline, fields, filter_=filter_, field_weights = field_weights)

class RecordLSH():

    threshold = 0.6
    num_perm = 128
    encoding = 'UTF-8'

    def __init__(self, pipeline, fields, filter_={}, field_weights = None):

        self.fields = fields
        self.filter_ = filter_
        self.p = pipeline

        self.wc = None
        self.gc = None
        self.pp = None
            
        if(not field_weights):
            field_weights = dict(zip(fields, [1]*len(fields)))
        self.field_weights = field_weights

        try:
            assert False, 'Use Factory'
        except AssertionError:  

            self.LSH = {}
            self.keys = {}
            self.field_weights = field_weights or dict(zip(fields, [1]*len(fields)))
            self.__totalweight__ = sum(self.field_weights.values())

            self.__build_index__()
            
            #Computationally expensive preprocssing to build hash table            
            for i, record in enumerate(self.p.client()[self.p.database][self.p.table].find(filter_,dict(zip(fields,[1]*len(fields))))):
                self.keys[i] = record['_id']
                for field in fields:
                    minhash = self.get_minhash(record.get(field,''))#[field])
                    try:
                        self.LSH[field].insert(i,minhash)
                    except KeyError:
                        self.LSH[field] = MinHashLSH(RecordLSH.threshold,RecordLSH.num_perm)
                        self.LSH[field].insert(i,minhash)

            self.p = self.p.clone()
##            print('LSH keys = {}'.format(self.LSH))
            self.__save__()
         
    def __save__(self):
        broker = Broker(self.p)
##        print('SAVING OBJ AS {}'.format(self.__getname__()))
        broker.upload_obj(self,self.__getname__())
        for key in self.LSH:
            broker.upload_obj(self.LSH[key], '{}_{}'.format(self.__getname__(),key)) 
    
    def __getname__(self):
        filter_tags = []
        for x in self.filter_:
            filter_tags.append('{}={}'.format(x,self.filter_[x]))
        if(len(self.filter_) == 0):
            return 'RecordLSH_{}'.format('_'.join(self.fields))
        return 'RecordLSH_{}_by_{}'.format('_'.join(self.fields),
                                           '_'.join(filter_tags) if len(filter_tags) > 0 else '')
    def __build_index__(self):
        field_keys = set(self.field_weights.keys())
        try:
            field_keys.remove('_id')
        except KeyError: pass
        self.p.build_index(self.p.table, field_keys)       

    def get_minhash(self, val):
        minhash = MinHash(RecordLSH.num_perm)
        for d in self.p.get_tri_grams(val):
            minhash.update(d.encode(RecordLSH.encoding))
        return minhash

    def match_by_field(self, other, field):
        return self.LSH[field].query(self.get_minhash(other[field]))

    def get_canidate_matches(self, other):
        canidates = set()
        for field in self.fields:
            for canidate in self.match_by_field(other, field):
                canidates.add(canidate)
        for canidate in canidates:
            yield self.p.client()[self.p.database][self.p.table].find_one({'_id':self.keys[canidate]})
                
    def bow_sim(self, field, r0, r1):

        def get_meta(rec):
            try: return rec['_meta'][field]
            except KeyError:

                if(not self.pp):
                    self.pp = PreprocessPiper(self.p)
##                if(not self.pp.wc):
##                    self.pp.wc = self.pp.__downloadcount__('wordcount')
##                if(not self.pp.gc):
##                    self.pp.gc = self.pp.__downloadcount__('gramcount')

                self.pp.__buildmetadata__(rec, self.fields)#, self.pp.wc, self.pp.gc)

                return rec['_meta'][field]
                print('_meta' in r0)
                print('_meta' in r1)
                raise Exception()
                pass

        recs = (r0,r1)
        meta = list(map(get_meta, recs))          
        words = set(itertools.chain(*[[word for word in rec[field].split()] for rec in recs]))

        v0, v1 = [[],[]]
        val_words0 = meta[0][recs[0][field]]
        val_words1 = meta[1][recs[1][field]]

        for word in words:
            def _sim(x): return [x, string_sim(word, x)]
           
            key0 = [word,1] if word in val_words0.keys() else max(map(_sim,val_words0.keys()),key=second)

            def _m():
                try:
                    return max(map(_sim,val_words1.keys()),key=second)
                except ValueError: return [word, 0]
               
            key1 = [word,1] if word in val_words1.keys() else _m()

            v0.append(val_words0[key0[0]]['count']*(1-val_words0[key0[0]]['freq'])*key0[1])
            try:
                v1.append(val_words1[key1[0]]['count']*(1-val_words1[key1[0]]['freq'])*key1[1])
            except KeyError: v1.append(0)
                
        return cosine_sim(v0,v1) 

   
    def match(self, other, thresh=0.0):
        canidates = list(self.get_canidate_matches(other))
        scored_recs = SortedList([],key=lambda x: -x[0])
        
        total_weight = sum(self.field_weights.values()) #normalize weight to be percent between 0-1
        def score_canidate(crec):
            field_sims = dict(zip(self.fields, map(lambda field: self.bow_sim(field, other, crec), self.fields)))
            score = sum(map(lambda field: ((self.field_weights[field]/total_weight)*field_sims[field]), self.field_weights)) 

            if(score > thresh):
                scored_recs.add([score,crec])

        [score_canidate(crec) for crec in canidates]
        return scored_recs.__iter__()

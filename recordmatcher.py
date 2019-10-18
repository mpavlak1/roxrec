#/roxrec/recordmatcher
'''
## ========================================================================== ##
- RecordMatcher encapsalates entire fuzzy record matching pipeline.
 .name = name of table that will be stored in DB
 .fields = list of 'fuzzy fields' to check similarity between,
   rather than requiring an absolute match
 .field_rename_map = specifies which fields in universe file correspond to
   field names in target file when field collumn names not equal
 .fuzzy_thresh = threshold to consider canidate with atleast minimum fuzzy similarity
 .field_weights = significance of each field
 .exact = list of field names that are required to match exactly,
   the more fields in exact, the more filitered the universe collection can be,
   resulting in both higher accuracy and faster runtime
- Match function will preform fuzzy matching on single record, using only a single process
- Matching by file will generate multiple processes to match records until target collection
   has been exhauseted. Matching by file significantly faster when doing bulk operations
## Author: Michael Pavlak
## ========================================================================== ##
'''

## Built-ins
import os
import sys
import subprocess
import threading

import time

## Package
import __init__
from general import *
from broker import Broker
from pipeline import Pipeline
from preprocesspiper import PreprocessPiper
from recordLSH import __LSHName__, RecordLSHFactory

## Additional
import pymongo.errors
from multiprocessing import cpu_count
from concurrent.futures import ThreadPoolExecutor

class RecordMatcher():

    def __init__(self, name, fields, universe_file = None, field_rename_map = {},
                 build_meta = False, fuzzy_thresh = 0.75, field_weights = None, exact = [],udelim='\t'):

        self.name = name
        
        self.fields = fields
        self.universe_file = universe_file
        self.field_rename_map = field_rename_map
        #if target has different names for same fields, specify and rename target fields when matching

        self.LSH = {}
        
        self.threshold = fuzzy_thresh
        self.field_weights = field_weights
        self.exact = exact

        self.__pipeline__ = Pipeline(name)
        self.__preprocessor__ = PreprocessPiper(self.__pipeline__)
        
        if(universe_file and os.path.exists(universe_file)):
            self.__pipeline__.connect()
            if(not self.__pipeline__.__client__[Pipeline.__database__]['meta_{}'.format(name)].find_one({'sha1':sha1_file(universe_file)})):
                try:
                    self.__preprocessor__.upload_universe_file(universe_file, build_meta = build_meta, delim=udelim)
                    self.__pipeline__.__client__[Pipeline.__database__]['meta_{}'.format(name)].insert_one(
                        {'sha1':sha1_file(universe_file),'source':universe_file})
                except pymongo.errors.BulkWriteError:
                    ##File might have changed, still uploads new records, does not upload duplicates
                    ##Throws error when any duplicates, any new records still get uploaded
                    pass
       
        self.filters = self.__getfilters__()

        for exit_status in self.__buildfilters__():
            if(exit_status[1] != 0):
                raise RuntimeError('When building {obj}, exit code returned non-zero ({code})'.format(
                    obj = exit_status[0], code = exit_status[1]))
        
    def __getfilters__(self, max_domain_size = 100, min_domain_size = 2):

    
        
        db = self.__pipeline__.client()[Pipeline.__database__][self.__pipeline__.table]
        def _get(e = self.exact):
            f = dict(zip(e, map(lambda x: '${}'.format(x), e)))
            return [i['_id'] for i in db.aggregate([{'$group': {'_id':f}}])]

        exact = list(self.exact)
        counts = list(map(lambda x: [x, len(db.distinct(x))], exact))
      
        for _ in range(len(self.exact)):
            filters = _get(exact)

            if(len(filters) > max_domain_size):
                exact.remove(max(counts, key = lambda x: x[1])[0])
            else:
                return filters
            
        raise Exception('To many filters')
     
        
    def __buildfilters__(self, worker_count = cpu_count()):

        start = time.time()

        threads = []
        db = self.__pipeline__.__client__[Pipeline.__database__]['{name}_meta_broker'.format(name = self.name)]
        with ThreadPoolExecutor(worker_count) as e:        
            for f in self.filters:                
                if(db.find_one({'_id' : hashstring(__LSHName__(self.fields, f))}, {'_id':1})): continue
                args = 'RecordLSHFactory(Pipeline("{}"), {}, filter_= {}, field_weights = {})'.format(
                    self.name, self.fields, f, self.field_weights)
                threads.append(e.submit(child_process, args=args))

        end = time.time()
        print('Total time elapsed: {} (seconds)'.format(end-start))

        return map(lambda i: [__LSHName__(self.fields, self.filters[i]), threads[i].result()], range(len(threads)))


    def match(self, record):

        for key in self.field_rename_map:
            rec[key] = rec[self.field_rename_map[key]]
        
        exact_match = self.__pipeline__.__client__[Pipeline.__database__][self.name].find_one(
            dict(zip(self.fields, map(lambda x: record[x], self.fields))))

        if(exact_match):
            try:
                del exact_match['_meta']
            except KeyError: pass

            exact_match['MATCH_RATE'] = 1
            return exact_match
        else:
            if('_meta' not in record):
            
                if(not self.__preprocessor__.wc): self.__preprocessor__.wc = self.__preprocessor__.__downloadcount__('wordcount')
                if(not self.__preprocessor__.gc): self.__preprocessor__.gc = self.__preprocessor__.__downloadcount__('gramcount')
                
                self.__preprocessor__.__buildmetadata__(record, self.fields, self.__preprocessor__.wc, self.__preprocessor__.gc)
            assert '_meta' in record
            return self.__fuzzymatch__(record)

    def __getfilteredLSH__(self, record):
        filter_ = {}
        for key in self.exact:
            try:
                filter_[key] = record[key]
            except KeyError: continue

        h = hashstring(__LSHName__(self.fields, filter_))
        try:
            return self.LSH[h]
        except KeyError:
            self.LSH[h] = RecordLSHFactory(self.__pipeline__, self.fields, filter_, self.field_weights)
            return self.LSH[h]

    def __fuzzymatch__(self,record):
        if('_meta' not in record.keys()):
            self.__preprocessor__.__buildmetadata__(record, self.fields, self.__preprocessor__.wc, self.__preprocessor__.gc)
        
        i = self.__getfilteredLSH__(record).match(record)
        
        best_match = next(i)
        best_match[1]['MATCH_RATE'] = best_match[0]       

        for key in tuple(best_match[1].keys()):
            if(key[0] == '_'):
                del best_match[1][key]

        return best_match[1]

    def match_file(self, target_file, outfile = None, name_remappings = {},
                   build_meta = False, delim='\t', worker_count = cpu_count()):
        outfile = outfile or '{}{}'.format(os.path.join(os.path.split(target_file)[0],
                                                        os.path.basename(target_file).split(os.path.extsep)[0]),
                                                        os.path.extsep.join(['_OUT','txt']))
        
        self.__preprocessor__.upload_target_file(target_file, build_meta = build_meta, delim=delim, name_remappings=name_remappings)

        workers = []
        def spawn_worker():
            init_args = '"{name}", {fields}, universe_file=None, field_rename_map={frm}, fuzzy_thresh={thresh}, field_weights={weights}, exact={e}'.format(
                name=self.__pipeline__.table, fields=self.fields, frm=self.field_rename_map, thresh=self.threshold, weights=self.field_weights, e=self.exact)
            i = 'RecordMatcher({args})'.format(args=init_args)     
            workers.append(child_process(i))

        threads = []
        for _ in range(worker_count): threads.append(threading.Thread(target=spawn_worker))
        for thread in threads: thread.start()
        for thread in threads: thread.join()

        ids = set()

        files = list(filter(lambda x: self.name in x, os.listdir(os.environ['TEMP'])))
        with open(outfile, mode='w',encoding='UTF-8',errors='ignore') as w:
            for file in files:
                file = os.path.join(os.environ['TEMP'], file)
                with open(file, mode='r',encoding='UTF-8',errors='ignore') as r:
                    for line in r.readlines():

                        #Mongo not consistent on concurrent find_and_remove_one calls
                        #Less strict consistency requirements allows Mongo speed up,
                        #Just check for duplicate records on merge
                        _id = line.split(delim)[0]
                        if(_id in ids): continue
                        else: ids.add(_id) 

                        w.write(line)
        
        for file in files:
            pass
            #print(file)
            #try: os.remove(os.path.join(os.environ['TEMP'], file))
            #except FileNotFoundError: continue

        assert all(map(lambda x: x==0, workers)), 'Some workers exited with errors'
        return outfile
 
    def __proceesstargets__(self, out = None):

        m = []      
        if(not out):
            out = os.path.join(os.environ['TEMP'], 'out_{}{pid}{ext}txt'.format(r.name, pid=os.getpid(), ext = os.path.extsep))

        rec = self.__pipeline__.client()[Pipeline.__database__]['{}_target'.format(self.name)].find_one_and_delete({})
        while(rec):
            try:
                rec_match = self.match(rec)
            except StopIteration: rec_match = {}

            try: del rec['_meta']
            except KeyError: pass

            try: del rec_match['_meta']
            except KeyError: pass
        
            if(rec_match != {}):
                m.append({'init':rec, 'match': rec_match})  
            rec = self.__pipeline__.__client__[Pipeline.__database__]['{}_target'.format(self.name)].find_one_and_delete({})

        with open(out,mode='w',encoding='UTF-8',errors='ignore') as w:

           m0 = max(m,key=lambda x: -len(x.keys()))
           w.write('{}\t===\t{}\t@@@\t{}\n'.format('\t'.join(m0['init'].keys()),
                                                     '\t'.join(filter(lambda x: x!='MATCH_RATE', m0['match'].keys())),
                                                     'MATCH_RATE'))

       
           
           for match in m:
               if(match['match'].get('MATCH_RATE',0) <= 0):
                   continue
               
               init = '\t'.join(map(str,match['init'].values()))
               mrec = '\t'.join(map(lambda x: str(match['match'][x]), filter(lambda key: key!='MATCH_RATE', match['match'].keys())))
               try:
                   w.write('{}\t===\t{}\t@@@\t{}\n'.format(init, mrec, match['match']['MATCH_RATE']))
               except KeyError:
                   #can log non-matched recs here if want
                   continue
        
        return out

    def __purge__(self):
        tables = ['{}'.format(self.name),'{}_target'.format(self.name),
                  '{}_meta_wordcount'.format(self.name),'{}_meta_gramcount'.format(self.name),
                  '{}_meta_broker'.format(self.name),'meta_{}'.format(self.name),'fs.chunks','fs.files']
        for table in tables: self.__pipeline__.__client__[Pipeline.__database__][table].drop()


BELOW_NORMAL_PRIORITY_CLASS = 0x00004000
SW_MINIMIZE = 6
info = subprocess.STARTUPINFO()
info.dwFlags = subprocess.STARTF_USESHOWWINDOW
info.wShowWindow = SW_MINIMIZE

def child_process(args):
    return subprocess.call(['python',__file__, args],
                           startupinfo = info, creationflags = BELOW_NORMAL_PRIORITY_CLASS)

### ========================================================================================= ###
### FORK PROCESS AND DO FUNCTION CALL
### ========================================================================================= ###

if(__name__ == '__main__'):
    try:
        arg = sys.argv[1]
        if(arg[0:16] == 'RecordLSHFactory'):
            eval(arg)
            sys.exit(0)
        if(arg[0:13] == 'RecordMatcher'):
            r = eval(arg)
            r.__proceesstargets__()
            sys.exit(0)
    except IndexError: pass


#r = RecordMatcher('hosp',['NAME','ADDRESS'],universe_file=r'Q:\Data\IRS\Form990_Healthcare_Names\FORM990_HEALTHCARE_NAMES_FORMATTED.txt',field_weights={'NAME':85,'ADDRESS':15},exact=['STATE'])

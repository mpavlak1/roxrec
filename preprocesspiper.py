#/roxrec/preprocesspiper.py
'''
## ========================================================================== ##
- PreprocessPiper class used to initialize data in MongoDB and additional
  task required prior to preforming fuzzy matching on records
- Preprocesses and uploads universe of potential records that targets can
  potentially be mapped to and generates index for fields to be matched.
- Preprocesses and uploads list of target records that will be matched to
  best match in universe collection
## Author: Michael Pavlak
## ========================================================================== ##
'''

##Built-ins
import threading

## Package
import __init__
from general import *
from pipeline import Pipeline


class PreprocessPiper():   

    def __init__(self, pipeline):
        self.p = pipeline
        self.wc = None #word count, will either be generated from universe or pulled from DB
        self.gc = None #gram count, will either be generated from universe or pulled from DB

    def __readuniversefile__(self, filepath, delim = '\t'):
        '''Read universe file and build wordcount and gramcount dictionaries in place from each line in file'''
        clean = self.p.clean #define clean on local score to avoid multiple lookups on self.p.cleans
        hashrec = self.p.hashrec

        recs = []
        with open(filepath,mode='r',encoding='UTF-8',errors='ignore') as r:
            header = clean(r.readline()).split(delim)

            word_count = {}
            gram_count = {}

            rec_count = 0
            rec_hashes = set()

            for line in r.readlines():
                cline = clean(line).split(delim)
                cline = cline + ['']*(len(header)-len(cline))
                rec = dict(zip(header,cline))
                _hash = hashrec(rec)
                
                if(_hash in rec_hashes): continue

                meta_field = {}
                for key in rec:
                    field_val = rec[key]
                   
                    try: word_count[key]
                    except KeyError: word_count[key] = {}

                    try: gram_count[key]
                    except KeyError: gram_count[key] = {}
                    
                    words = {}
                    for word in field_val.split():
                        try: word_count[key][word] += 1
                        except: word_count[key][word] = 1

                        try: words[word]['count'] += 1
                        except KeyError: words[word] = {'count' : 1}                               

                        for gram in self.p.get_tri_grams(word):
                            try: gram_count[key][gram] += 1
                            except KeyError: gram_count[key][gram] = 1

                            try: words[word][gram]['count'] += 1
                            except KeyError: words[word][gram] = {'count' : 1}
                    try: meta_field[key][field_val] = words
                    except KeyError: meta_field[key] = {field_val : words}              

                rec['_id'] = _hash
                rec['_meta'] = meta_field

                recs.append(rec)
                rec_count +=1

##                if(rec_count > 100000):
##                    print('MANUAL GARBAGE COLLECTION CALLED')
##                    gc.collect()
##                    rec_count = 0
                

##            print('DONE READLING LINES ({})'.format(len(recs)))

            rec_count = 0
            
            for i in range(len(recs)):

##                if(rec_count > 100000):
##                    print('MANUAL GARBAGE COLLECTION CALLED')
##                    gc.collect()
##                    rec_count = 0
                
                for key in rec:
                    if(key[0] == '_'): continue

                    try:
                        field_val = recs[i][key]
                    except Exception:
                        print(key)
                        print(recs[i].keys())
                        print(recs[i])
                        
                        raise
                                        
                    for word in field_val.split():
                        
                        recs[i]['_meta'][key][field_val][word]['freq'] = word_count[key][word]/len(word_count[key])

                        #((len(word_count[key])/word_count[key][word])/recs[i]['_meta'][key][field_val][word]['count'])/len(word_count[key])

                        for gram in recs[i]['_meta'][key][field_val][word]:
                            if(len(gram) != 3): continue

                            L = len(gram_count[key])
                            g = gram_count[key][gram]
                            #s = recs[i]['_meta'][key][field_val][word][gram]['count']
                            
                            recs[i]['_meta'][key][field_val][word][gram]['freq'] = g/L
                rec_count += 1

        self.__update_count__(word_count, 'wordcount')
        self.__update_count__(gram_count, 'gramcount')
        return recs

    
    def __downloadcount__(self, count_name):
        collection_name = '{table}_meta_{count_name}'.format(table=self.p.table, count_name = count_name)
        db = self.p.client()[self.p.database][collection_name]
       
        count = {}
        for key in db.distinct('field'):
            count[key] = {}
            for rec in db.find({'field':key}):
                count[key][rec[count_name]] = rec['count']
        return count


    def __buildmetadata__(self, record, fields, wc, gc):
        meta = {}

        for field in fields:
            try:
                field_val = record[field]
            except KeyError: continue
            meta[field] = {field_val : {}}

            global_wc_len = len(wc[field])
            global_gc_len = len(gc[field])

            for word in field_val.split():
                word_count = wc[field].get(word, 1)

                try: meta[field][field_val][word]['count'] += 1
                except KeyError:
                    meta[field][field_val][word] = {'count':1, 'freq':word_count/global_wc_len}

                for gram in self.p.get_tri_grams(word):
                    gram_count = gc[field].get(gram,1)

                    try: meta[field][field_val][word][gram]['count'] += 1
                    except KeyError:
                        meta[field][field_val][word][gram] = {'count':1, 'freq':gram_count/global_gc_len}
        record['_meta'] = meta

    def __readtargetfile__(self, filepath, delim='\t', name_remappings = {}):
        clean = self.p.clean
        hashrec = self.p.hashrec

        recs = []

        if(not self.wc): self.wc = self.__downloadcount__('wordcount')
        if(not self.gc): self.gc = self.__downloadcount__('gramcount')

        fields = self.p.client()[self.p.database][self.p.table].find_one({},{'_id':0,'_meta':0}).keys()

        rec_hashes = set()
        with open(filepath,mode='r',encoding='UTF-8',errors='ignore') as r:
            header = clean(r.readline()).split(delim)
            for line in r.readlines():
                cline = clean(line).split(delim)
                cline = cline + ['']*(len(header)-len(cline))
                rec = dict(zip(header,cline))

                for key in name_remappings:
                    try:
                        rec[key] = rec[name_remappings[key]]
                        del rec[name_remappings[key]]
                    except KeyError: continue

                _hash = hashrec(rec)
                if(_hash in rec_hashes): continue
                rec['_id'] = _hash
                self.__buildmetadata__(rec, fields, self.wc, self.gc)
                
                recs.append(rec)
        
        return recs

    def upload_target_file(self, filepath, delim='\t', name_remappings = {}):
        recs = self.__readtargetfile__(filepath, delim=delim, name_remappings = name_remappings)

        tbl_name = '{}_target'.format(self.p.table)
    
        #self.p.client()[self.p.database][tbl_name].drop()
##        print('UPLOADING {} TARGET RECORDS'.format(len(recs)))
        if(len(recs) > 0):
            try:
                self.p.client()[self.p.database][tbl_name].insert_many(recs, ordered=False)
            except pymongo.errors.BulkWriteError: pass
     
    def upload_universe_file(self, filepath):
        recs = self.__readuniversefile__(filepath)#[x for x in self.__readfile__(filepath)]
##        print('UPLOADING {} RECORDS'.format(len(recs)))
        self.p.client()[self.p.database][self.p.table].insert_many(recs, ordered=False)

    def __update_count__(self, additional_counts, count_type):
        hashrec = self.p.hashrec
        count_table = '{}_meta_{}'.format(self.p.table,count_type)

        d = []
        hashes = set()
        for field in additional_counts.keys():
            for elem in additional_counts[field]:
                r = {'field':field, count_type:elem, 'count':additional_counts[field][elem]}
                _hash = hashrec(r)
                if(_hash not in hashes):
                    hashes.add(_hash)
                    r['_id'] = _hash
                    d.append(r)

        #Spawn new thread, let Mongo handle concurrency, make client call non-blocking                 
        threading.Thread(target = lambda: self.p.client()[self.p.database][count_table].insert_many(d, ordered=False)).start()
        threading.Thread(target = lambda: self.p.build_index(count_table,['field','count',count_type])).start()

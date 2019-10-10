# /roxrec/broker.py
'''
## ========================================================================== ##
## Broker class used to manage distributed objects
##  - serializes and stores python objects either directly in MongoDB
##    or on Grid File System on disk
## - Metadata about objects include name and storeage location
## - All objects stored using broker required to have unique name
## - Name used to identify objects, required for lookup and downloads
## - Name set on upload
## Author: Michael Pavlak
## ========================================================================== ##
'''

## Package
import __init__
from general import *

##Additional
import gridfs
import dill as pickle


class Broker():

    def __hash__(self, s): return hashstring(str(s))

    def __init__(self, pipeline):
        #pipeline is Pipeline object initialized with valid argument to create MongoDB client connection
        self.p = pipeline 

    def __upload_big__(self, serialized_obj, name):
        fs = gridfs.GridFS(self.p.client()[self.p.database])
        file_id = fs.put(serialized_obj)
        r = {'_id': self.__hash__(name),
             'name':name,
             'file_id':file_id}
        self.p.client()[self.p.database]['{}_meta_broker'.format(self.p.table)].insert_one(r)

    def upload_obj(self, obj, name):
        try:
            serialized_obj = pickle.dumps(obj, recurse = True)
        except Exception as e:
            serialized_obj = pickle.dumps(obj, recurse = False)
        try:
            r = {'_id': self.__hash__(name),
                 'name':name,
                 'obj':serialized_obj}
            
            self.p.client()[self.p.database]['{}_meta_broker'.format(self.p.table)].insert_one(r)
        except Exception as e:
            self.__upload_big__(serialized_obj, name)
                
    def __download_big__(self, file_id):
        fs = gridfs.GridFS(self.p.client()[self.p.database])
        return pickle.loads(fs.get(file_id).read())

    def download_obj(self, name):
        rec = self.p.client()[self.p.database]['{}_meta_broker'.format(self.p.table)].find_one({'_id':self.__hash__(name)})   
        assert rec
        if('obj' in rec):
            return pickle.loads(rec['obj'])
        elif('file_id' in rec):
            return self.__download_big__(rec['file_id'])
        else: raise KeyError

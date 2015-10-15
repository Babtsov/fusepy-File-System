from pymongo import  MongoClient
from bson.objectid import ObjectId
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import time
from collections import OrderedDict


class FSMongoClient(object):
    """
    This class manages the communications with the remote database, which stores the file system.
    Object Properties:
    self.fs_collection: stores a reference to the MongoDB collection
        in which all the files are stored
    self.root_id: stores the ObjectId of the document representing the root
    """
    def __init__(self,url,port):
        # retrieve the collection FUSEPY_FS from the FS_DB database
        self.fs_collection = MongoClient(url,port).FS_DB.FUSEPY_FS
        fs_root = self.fs_collection.find_one({"name": '/'})
        if fs_root:     # File system root exists already
            print 'Root exists. loading existing root...'
            self.root_id = fs_root['_id']
        else:           # no root is defined yet, so insert it.
            print 'No root exists. Creating a new root...'
            now = time()
            meta_data = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)
            fs_root = dict(name='/',type='dir',meta=meta_data,data={})
            self.root_id = self.fs_collection.insert_one(fs_root).inserted_id

    def id_lookup(self,file_id):
        # Retrieve a file from the DB using its _id. the _id must be an object of type ObjectId
        assert type(file_id) == ObjectId
        return self.fs_collection.find_one({'_id': file_id})

    def insert_new_file(self,new_file_dict):
        # Insert a file to the DB. all the file contents should be in new_file_dict.
        # Returns the _id of the newly inserted item.
        assert {'name','type','meta','data'} == set(new_file_dict.keys())
        return self.fs_collection.insert_one(new_file_dict).inserted_id

    def update_file(self,file_id,field_to_update,field_content):
        # Update a certain file's property. the property must be one of the following:
        # name, type, meta, data. field_content is the new value of the file property
        assert field_to_update in ['name','type','meta','data']
        self.fs_collection.update_one({"_id":file_id},
                                      {"$set": {field_to_update:field_content}})
        
    def remove_file(self,file_id):
        # Remove a file from the DB by supplying its _id
        assert type(file_id) == ObjectId
        self.fs_collection.remove({'_id' : file_id})

    def print_db(self): # used for debugging
        i = 1
        print "DATABASE CONTENT:\n\t BEGIN DB LIST:"
        for document in self.fs_collection.find():
            print "------------ {0} -----------".format(i)
            print "_id: ",document['_id']
            print "name: ",document['name']
            print "type: ",document['type']
            print "data: ",document['data']
            i += 1
        print "\t END DB LIST"


class LRUCache(object):
    """
    This cache keeps the most recently used key value pairs in a queue based on get requests.
    The queue is managed internally by OrderedDict.
    The cache raises keyError exception if key is not present.
    Set requests (setting a value to an existing key) don't move the entry up the queue
    """
    def __init__(self, max_size=100):
        self.capacity = max_size
        self.data = OrderedDict()

    def __getitem__(self, item):
        if type(item) == ObjectId:
            item = str(item)
        value = self.data.pop(item) # raises keyError exception if not in cache
        self.data[item] = value
        return value

    def __setitem__(self, key, value):
        if type(key) == ObjectId:
            key = str(key)
        if self.capacity == len(self.data): # if we reached max capacity,
                self.data.popitem(last=False)   # then pop the least recently used
        self.data[key] = value

    def __delitem__(self, key):
        if type(key) == ObjectId:
            key = str(key)
        if key in self.data:
            del self.data[key]

    def __str__(self):
        return "{MAX_SIZE: "+ str(self.capacity) + ". ACTUAL SIZE: " + str(len(self.data)) \
               + "\n\t CONTENT: " + str(self.data) + "}"


class FileStorageManager(object):
    def __init__(self,db_url,db_port,cache_size):
        self.db = FSMongoClient(db_url,db_port)
        self.cache = LRUCache(cache_size)

    def retrieve_by_id(self,file_id):
        """
        Tries to retrieve the requested file by id from the cache.
        If failed, tries to retrieve it from the database, and stores the response from the
        database in the cache. The response can be either the file requested or None.
        """
        try:
            return self.cache[file_id]
        except KeyError:
            db_output = self.db.id_lookup(file_id) # cache the DB output whther it's a returned file or None
            self.cache[file_id] = db_output
            return db_output

    def lookup(self,path): # retrieve a file from the DB or cache using its path (type is str)
        root_file = self.retrieve_by_id(self.db.root_id)
        if path == '/':
            return root_file
        path_parts = path.split("/")[1:] # [1:] to get rid of the first element ''
        if path_parts[-1] == '': # path_parts[-1] will be '' if path ends with / (./a/b/)
            path_parts.pop() # remove it so we won't be iterating through an empty name
        context = root_file
        for name in path_parts:
            try:
                file_id = root_file['data'][name]
            except KeyError:
                return None
            file_doc = self.retrieve_by_id(file_id)

            if file_doc['type'] == 'dir': # we have a directory
                context = file_doc # set the directory as the new context for the lookup
            else:
                return file_doc # we have a regular file, so return it
        # if we reached this point, it means that the requested file is a dir, so return it
        return context

    def update(self,file_id):
        pass

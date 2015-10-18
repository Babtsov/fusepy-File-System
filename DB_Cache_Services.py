from pymongo import MongoClient
from bson.objectid import ObjectId
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import time
from collections import OrderedDict
from fuse import FuseOSError
from errno import ENOENT


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

    def insert_file(self,new_file_dict):
        # Insert a file to the DB. all the file contents should be in new_file_dict.
        # This method will modify new_file_dict provided to
        # include a new property '_id' since the dict now represent a file that was inserted into the database.
        assert {'name','type','meta','data'} == set(new_file_dict.keys())
        self.fs_collection.insert_one(new_file_dict)

    def update_file(self,file_id,field_to_update,field_content):
        # Update a certain file's property. the property must be one of the following:
        # name, type, meta, data. field_content is the new value of the file property
        assert field_to_update in ['name','type','meta','data']
        self.fs_collection.update_one({"_id":file_id},
                                      {"$set": {field_to_update:field_content}})

    def remove_file(self,file_id):
        # Remove a file from the DB by supplying its _id
        assert type(file_id) == ObjectId
        self.fs_collection.delete_one({'_id' : file_id})

    def print_db(self): # used for debugging
        print "DATABASE CONTENT\n\t BEGIN DB LIST:"
        for index, document in enumerate(self.fs_collection.find()):
            print "------------ {0} -----------".format(index)
            print "_id: ",document['_id']
            print "name: ",document['name']
            print "type: ",document['type']
            print "data: ",document['data']
        print "\t END DB LIST"


class LRUCache(object):
    """
    This cache keeps the most recently used key value pairs in a queue based on get requests.
    The queue is managed internally by OrderedDict.
    The internal OrderedDict uses str as a key, but the input to the cache should be object of type ObjectId
    The cache raises keyError exception if key is not present. or FuseOSError(ENOENT) if the value that
    corresponds to the key is None
    Set requests (setting a value to an existing key) don't move the entry up the queue
    """
    def __init__(self, max_size=100):
        self.capacity = max_size
        self.data = OrderedDict()

    def __getitem__(self, item):
        assert type(item) == ObjectId
        item = str(item)
        value = self.data.pop(item) # raises keyError exception if not in cache
        self.data[item] = value
        if not value:
            raise FuseOSError(ENOENT)
        return value

    def __setitem__(self, key, value):
        assert type(key) == ObjectId
        key = str(key)
        if self.capacity == len(self.data): # if we reached max capacity,
            self.data.popitem(last=False)   # then pop the least recently used
        self.data[key] = value

    def __delitem__(self, key):
        assert type(key) == ObjectId
        key = str(key)
        if key in self.data:
            del self.data[key]

    def print_cache(self):
        print "CACHE CONTENT"
        print "MAX_SIZE: "+ str(self.capacity) + ". ACTUAL SIZE: " + str(len(self.data))
        print "\t BEGIN CACHE LIST: "
        for index, document in enumerate(self.data.values()):
            print "$$$$$$$$$$$ {0} $$$$$$$$$$$$".format(index+1)
            print "_id: ",document['_id']
            print "name: ",document['name']
            print "type: ",document['type']
            print "data: ",document['data']
        print "\t END CACHE LIST"


class FileStorageManager(object):
    """
    A class to manage both the database and the cache. It uses the cache for fast 'get' accesses
    and synchronizes the cache and database whenever data is changed.
    """
    def __init__(self,db_url,db_port,cache_size):
        self.db = FSMongoClient(db_url,db_port)
        self.cache = LRUCache(cache_size)

    def _retrieve_file(self,file_id):
        """
        Tries to retrieve the requested file by id from the cache.
        If failed, tries to retrieve it from the database, and stores the response from the
        database in the cache. The response can be either the dict of the file requested or None.
        :param file_id: the ObjectId that corresponds to a file stored in the db or cache
        :return: dict with the following keys: ('_id', 'name', 'meta', 'type', 'data')
        """
        try:
            return self.cache[file_id]
        except KeyError:
            db_output = self.db.id_lookup(file_id) # cache the DB output whether it's a returned file or None
            self.cache[file_id] = db_output
            return db_output

    def lookup(self,path):
        """
        Retrieves a file from the DB or cache using its path. Raises a FuseOSError(ENOENT)
        if the file is not found.
        :param path: str representing FS path that corresponds to a file stored in the db or cache
        :return:dict with the following keys: ('_id', 'name', 'meta', 'type', 'data')
        """
        root_file = self._retrieve_file(self.db.root_id)
        if path == '/':
            return root_file
        path_parts = path.split("/")[1:] # [1:] to get rid of the first element ''
        if path_parts[-1] == '': # path_parts[-1] will be '' if path ends with / (./a/b/)
            path_parts.pop() # remove it so we won't be iterating through an empty name
        context = root_file
        for name in path_parts:
            try:
                file_id = context['data'][name]
            except KeyError:
                raise FuseOSError(ENOENT)
            file_doc = self._retrieve_file(file_id)
            if file_doc['type'] == 'dir': # we have a directory
                context = file_doc # set the directory as the new context for the lookup
            else:
                return file_doc # we have a regular file, so return it
        # if we reached this point, it means that the requested file is a dir, so return it
        return context

    def update_file(self,file_dict,field_to_update,field_content):
        """
        Updates the file associated with the path(str) with the new field_content.
        Both the database and the cache are updated with the new information.
        If the cache doesn't have the file, it is added to the cache.
        :param file_dict: a dictionary represents a file (as stored in the db and cache)
        The dictionary must have the following keys: ('_id', 'name', 'meta', 'type', 'data')
        :param field_to_update: must be a str of one of the following: 'name', 'meta', 'type', 'data'
        :param field_content: the new content to be inserted into one of the fields.
        """
        assert set(file_dict.keys()) == {'_id','name','type','meta','data'}
        del self.cache[file_dict['_id']] # remove the old entry from the cache (if it exists)
        file_dict[field_to_update] = field_content  # modify the dict of the file
        self.cache[file_dict['_id']] = file_dict  # update the cache
        self.db.update_file(file_dict['_id'],field_to_update,field_content)  # update the db

    def update_dir_data(self,dir_dict,**kwargs):
        """
        Updates the data of a directory, which represents the contents of this particular dir.
        The content is always a dictionary mapping file names (str) to _id (ObjectId)
        :param dir_dict: a dictionary represents a directory (as stored in the db and cache)
        The dictionary must have the following keys: ('_id', 'name', 'meta', 'type', 'data')
        :param **kwargs: 'action' must be supplied with the following '$add', '$modify', or '$rename'
            if action='$add' is supplied: 'child_dict' must also be supplied as one of the aguments
            child_dict is the dictionary that represents the child file.
            if action='$remove' is supplied: 'child_name' must also be supplied. 'child_name' is the
            name of the child file to be removed from the directory.
            if action='$rename' is supplied: 'new_name' and 'old_name' must be supplied too.
            those preresent the new and the old name of the child file to be renamed.
        """
        assert set(dir_dict.keys()) == {'_id','name','type','meta','data'}
        assert dir_dict['type'] == 'dir'
        del self.cache[dir_dict['_id']]
        file_data = dir_dict['data']
        if kwargs['action'] == '$add':
            try:
                child_dict = kwargs['child_dict']
            except KeyError:
                assert False, "child_dict kw argument must be supplied"
            assert set(child_dict.keys()) == {'_id','name','type','meta','data'}
            file_data[child_dict['name']] = child_dict['_id']

        elif kwargs['action'] == '$remove':
            assert 'child_name' in kwargs.keys(), 'No child_name is given for removal'
            del file_data[kwargs['child_name']]

        elif kwargs['action'] == '$rename':
            try:
                old_name, new_name = kwargs['old_name'], kwargs['new_name']
            except KeyError:
                assert False, "child old and new names must be supplied in the kwargs"
            child_id = file_data.pop(old_name)
            file_data[new_name] = child_id
        else:
            assert False, "Invalid action was provided."
        self.cache[dir_dict['_id']] = dir_dict
        self.db.update_file(dir_dict['_id'],'data',file_data)

    def insert_file(self,file_dict):
        """
        Inserts a new file to both the db and cache. This method will modify file_dict provided to
        include a new property '_id' since the dict now represent a file that was inserted into the database.
        :param file_dict: The dictionary representing the new file. It should have the following keys:
        ('name', 'meta', 'type', 'data')
        """
        self.db.insert_file(file_dict)
        self.cache[file_dict['_id']] = file_dict

    def remove_file(self,file_dict):
        """
        Deletes a file from both the db and the cache
        :param file_dic: a dictionary represents a file (as stored in the db and cache)
        The dictionary must have the following keys: ('_id', 'name', 'meta', 'type', 'data')
        """
        assert set(file_dict.keys()) == {'_id','name','type','meta','data'}
        file_id = file_dict['_id']
        self.db.remove_file(file_id)
        del self.cache[file_id]

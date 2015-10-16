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
        self.fs_collection.delete_one({'_id' : file_id})

    def print_db(self): # used for debugging
        print "DATABASE CONTENT:\n\t BEGIN DB LIST:"
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
    The cache raises keyError exception if key is not present.
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

    def retrieve_by_id(self,file_id):
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

    def lookup(self,path): #
        """
        Retrieves a file from the DB or cache using its path. Raises a FuseOSError(ENOENT)
        if the file is not found.
        :param path: str representing FS path that corresponds to a file stored in the db or cache
        :return:dict with the following keys: ('_id', 'name', 'meta', 'type', 'data')
        """
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
                raise FuseOSError(ENOENT)
            file_doc = self.retrieve_by_id(file_id)
            if file_doc['type'] == 'dir': # we have a directory
                context = file_doc # set the directory as the new context for the lookup
            else:
                return file_doc # we have a regular file, so return it
        # if we reached this point, it means that the requested file is a dir, so return it
        return context

    def update_file(self,path,field_to_update,field_content):
        """
        Updates the file associated with the path(str) with the new field_content.
        Both the database and the cache are updated with the new information.
        If the cache doesn't have the file, it is added to the cache.
        :param path: str representing FS path that corresponds to a file stored in the db or cache
        :param field_to_update: must be a str of one of the following: 'name', 'meta', 'type', 'data'
        :param field_content: the new content to be inserted into one of the fields.
        """
        file_dic = self.lookup(path)
        del self.cache[file_dic['_id']] # remove the old entry from the cache (if it exists)
        file_dic[field_to_update] = field_content  # modify the dict of the file
        self.cache[file_dic['_id']] = file_dic  # update the cache
        self.db.update_file(file_dic['_id'],field_to_update,field_content)  # update the db

    def update_dir_data(self,path,child_dict,action):
        """
        Updates the data of a directory, which represents the contents of this particular dir.
        The content is always a dictionary mapping file names (str) to _id (ObjectId)
        :param path: str representing FS path that corresponds to a directory stored in the db or cache
        :param child_dict: the dictionary of the directory's child. Should have the
        following keys: ('_id', 'name', 'meta', 'type', 'data')
        :param action: can be either '$add', '$modify', or '$remove'
        """
        file_dict = self.lookup(path)
        del self.cache[file_dict['_id']]
        assert file_dict['type'] == 'dir'
        file_data = file_dict['data']
        if action in ('$add','$modify'):
            file_data[child_dict['name']] = child_dict['_id']
        elif action == '$remove':
            del file_data[child_dict['name']]
        else : assert False, "Invalid action used"
        self.cache[file_dict['_id']] = file_dict
        self.db.update_file(file_dict['_id'],'data',file_data)

    def insert_file(self,file_dict):
        """
        Inserts a new file to both the db and cache
        :param file_dict: The dictionary representing the new file. It should have the following keys:
        ('name', 'meta', 'type', 'data')
        :return: the updated file dict which also includes the _id key and its corresponding value generated
        by the db
        """
        new_file_id = self.db.insert_file(file_dict)
        file_dict['_id'] = new_file_id
        self.cache[new_file_id] = file_dict
        return file_dict

    def remove_file(self,path):
        """
        Deletes a file from both the db and the cache
        :param path: str representing FS path that corresponds to a file stored in the db or cache
        """
        file_id = self.lookup(path)['_id']
        self.db.remove_file(file_id)
        del self.cache[file_id]

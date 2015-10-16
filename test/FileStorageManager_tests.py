# Before executing this code, run on another terminal the following:
# $ mongod --port 27027

from remote_services import FSMongoClient, FileStorageManager
from bson.objectid import ObjectId

def clear_db(url,port):
	from pymongo import  MongoClient
	MongoClient(url,port).FS_DB.FUSEPY_FS.drop() 

db_url,db_port = 'localhost',27027
clear_db(db_url,db_port) # clear the db before the test

storage_manager = FileStorageManager(db_url,db_port,5)

print "################## TEST 1 ###################"
print "Creates 2 new files and adds them under root"
storage_manager.insert('/',dict(name='Loco',type='reg',meta='M',data='HEY!'))
storage_manager.insert('/',dict(name='dir1',type='dir',meta='M',data={}))
storage_manager.db.print_db()
storage_manager.cache.print_cache()

print "################## TEST 2 ###################"
print "Removes dir1 from the file system"
storage_manager.remove('/dir1')
storage_manager.db.print_db()
storage_manager.cache.print_cache()

print "################## TEST 3 ###################"
print "Changes the name of the Loco file to Coco"
storage_manager.update('/Loco','name','Coco')
storage_manager.db.print_db()
storage_manager.cache.print_cache()
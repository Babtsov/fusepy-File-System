# Before executing this code, run on another terminal the following:
# $ mongod --port 27027

from remote_services import FileStorageManager

def clear_db(url,port):
	from pymongo import  MongoClient
	MongoClient(url,port).FS_DB.FUSEPY_FS.drop() 


def print_db_and_cache(storage_manager):
	storage_manager.db.print_db()
	storage_manager.cache.print_cache()


db_url,db_port = 'localhost',27027
clear_db(db_url,db_port) # clear the db before the test cases

storage_manager = FileStorageManager(db_url,db_port,5)
root_dict = storage_manager.lookup('/')
print "################## TEST 1 ###################"
print "Creates 2 new files and adds them under root"
loco_dict = storage_manager.insert_file(dict(name='Loco',type='reg',meta='M',data='HEY!'))
dir1_dict = storage_manager.insert_file(dict(name='dir1',type='dir',meta='M',data={}))
storage_manager.update_dir_data(root_dict,loco_dict,'$add')
storage_manager.update_dir_data(root_dict,dir1_dict,'$add')
print_db_and_cache(storage_manager)

print "################## TEST 2 ###################"
print "Removes dir1 from the file system"
dir1_dict = storage_manager.lookup('/dir1')
storage_manager.remove_file(dir1_dict)
storage_manager.update_dir_data(root_dict,dir1_dict,'$remove')
print_db_and_cache(storage_manager)

clear_db(db_url,db_port) # clear the db after the test cases

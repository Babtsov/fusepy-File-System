# Before executing this code, run on another terminal the following:
# $ mongod --port 27027

from remote_services import FSMongoClient
from bson.objectid import ObjectId

def clear_db(url,port):
	from pymongo import  MongoClient
	MongoClient(url,port).FS_DB.FUSEPY_FS.drop() 

db_url,db_port = 'localhost',27027
clear_db(db_url,db_port) # clear the db before the test

client = FSMongoClient(db_url,db_port)
print "################## TEST 1 ###################"
print "Creates 2 new files and adds them under root"
# add new files to db
Loco_file_dict = dict(name='Loco',type='reg',meta='M',data='HEY!')
Loco_file_id = client.insert_new_file(Loco_file_dict)

dir1_file_dict = dict(name='dir1',type='dir',meta='M',data={})
dir1_file_id = client.insert_new_file(dir1_file_dict)

# add the new files under root
root_data = client.id_lookup(client.root_id)['data']
root_data[dir1_file_dict['name']] = dir1_file_id
root_data[Loco_file_dict['name']] = Loco_file_id
client.update_file(client.root_id,'data',root_data)
client.print_db()

print "################## TEST 2 ###################"
print "Removes dir1 from the file system"
client.remove_file(dir1_file_id)
root_data = client.id_lookup(client.root_id)['data']
del root_data[dir1_file_dict['name']]
client.update_file(client.root_id,'data',root_data)
client.print_db()

clear_db(db_url,db_port) # clear the db after the test
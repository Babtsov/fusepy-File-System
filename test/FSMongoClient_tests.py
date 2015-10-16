# Before executing this code, run on another terminal the following:
# $ mongod --port 27027

from remote_services import FSMongoClient

def clear_db(url,port):
	from pymongo import  MongoClient
	MongoClient(url,port).FS_DB.FUSEPY_FS.drop() 

db_url,db_port = 'localhost',27027
clear_db(db_url,db_port) # clear the db before the test

print "################## TEST 1 ###################"
client = FSMongoClient(db_url,db_port)
# add new file to db
file_dict = dict(name='Loco',type='reg',meta='M',data='HEY!')
new_file_id = client.insert_new_file(file_dict)

# add the new file under root
client.update_file(client.root_id,'data',{file_dict['name']: new_file_id})

client.print_db()
print "################## END TEST 1 ################"

clear_db(db_url,db_port) # clear the db after the test
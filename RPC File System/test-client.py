# this is a simple client that makes use of the server given by simpleht.py
from pickle import dumps, loads
from xmlrpclib import Binary, ServerProxy

rpc = ServerProxy('http://localhost:8080')

def simpleht_send(key,data):
	rpc.put(Binary(key),Binary(dumps(data)),3000)


def simpleht_recieve(key):
	pickled_obj = rpc.get(Binary(key))["value"].data
	return loads(pickled_obj)


def store_retrive_print(key,data):
	simpleht_send(key,data)
	recived_data = simpleht_recieve(key)
	print "recived_data content is: {0}, and its type is: {1}".format(recived_data,type(recived_data))


def main():
	# store and retrive an integer:
	store_retrive_print("my_int",8)

	# store and retrive a string:
	store_retrive_print("my_str","Hello there!")

	# store and retrive a list:
	store_retrive_print("my_list",[1,89,667])

	# store and retrive a dictionary:
	store_retrive_print("my_dict",dict(a=1,b=2,c=3))

	# save/restore data to/from the filesystem
	rpc.write_file(Binary("./test.txt"))
	rpc.read_file(Binary("./test.txt"))

	rpc.print_content()


if __name__ == '__main__':
	main()

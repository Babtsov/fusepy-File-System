#!/usr/bin/env python

import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib
from xmlrpclib import Binary
from FileSystem import File

class Server:
  def __init__(self):
    self.data = {}

  def get(self, key):
    key = key.data
    if key in self.data:
      return Binary(self.data[key])
    else:
      return False

  def put(self, key, value):
    self.data[key.data] = value.data
    return True

  def delete(self, key):
    del self.data[key.data]
    return True

  def print_content(self):
    print "~~~~~~~~~~~~~ Content ~~~~~~~~~~~~~"
    for key,val in self.data.items():
        print "(",str(key),", ",str(pickle.loads(val)),")"
    print "~~~~~~~~~~~ End content ~~~~~~~~~~~"
    return True


# Start the server
def serve(port):
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port))
  file_server.register_introspection_functions()
  sht = Server()
  file_server.register_function(sht.get)
  file_server.register_function(sht.put)
  file_server.register_function(sht.delete)
  file_server.register_function(sht.print_content)
  file_server.serve_forever()


if __name__ == "__main__":
  port = sys.argv[1]
  serve(int(port))
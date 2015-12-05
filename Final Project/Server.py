#!/usr/bin/env python

import sys, pickle
from xmlrpclib import Binary
from SimpleXMLRPCServer import SimpleXMLRPCServer


class Server(object):
    def __init__(self,host_name,port,corruptible):
        self.data = {}
        self.corruptible = corruptible
        self.continue_running = True
        self.server = SimpleXMLRPCServer((host_name, port))
        self.server.register_instance(self)
        while self.continue_running:
            self.server.handle_request()

    def get(self, key):
        if key.data in self.data:
            return Binary(self.data[key.data])
        return False

    def put(self, key, value):
        self.data[key.data] = value.data
        return True

    def delete(self, key):
        del self.data[key.data]
        return True

    def list_contents(self): # used for debugging
        return Binary(pickle.dumps(self.data))

    def corrupt(self,key): # used for debugging
        if not self.corruptible:
            return False
        if key.data in self.data:
            self.data[key.data] = "CORRUPT!"
            return Binary("CORRUPT!")
        return False

    def terminate(self): # used for debugging
        self.continue_running = False
        return True


if __name__ == "__main__":
    port = sys.argv[1]
    Server("localhost",int(port),True)
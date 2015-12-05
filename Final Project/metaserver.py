from __future__ import print_function
from Server import Server
from multiprocessing import Process
from sys import argv,exit

if __name__ == "__main__":
    try:
        if len(argv) != 2: raise ValueError
        p = Process(target=Server,args=("localhost",int(argv[1]),False))
        p.daemon = True
        p.start()
    except ValueError:
        print("Usage: python metaserver.py <port>")
        exit(1)
    print("Enter quit to exit the server.")
    while True:
        if raw_input("") == "quit": break
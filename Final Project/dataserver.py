from __future__ import print_function
from Server import Server
from multiprocessing import Process
from sys import argv,exit

if __name__ == "__main__":
    try:
        if len(argv) < 2: raise ValueError
        for arg in argv[1:]:
            print("Started data server on port:",arg)
            p = Process(target=Server,args=("localhost",int(arg),True))
            p.daemon = True
            p.start()
    except ValueError:
        print("Usage: python dataserver.py <port 1> <port 2> ..<port n>")
        exit(1)
    print("Enter quit to exit all servers.")
    while True:
        if raw_input("") == "quit": break
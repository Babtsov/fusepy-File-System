from __future__ import print_function
from xmlrpclib import Binary, ServerProxy
import pickle, socket
import os.path, sys
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
from FileSystem import File


def show_content(server):
    content = pickle.loads(server.list_contents().data)
    print("~~~~~~~~~~~~~ Content ~~~~~~~~~~~~~")
    for key,val in content.items():
        if val == "CORRUPT!":
            print("("+str(key)+", "+"CORRUPT!"+")")
        else:
            print("("+str(key)+", "+str(pickle.loads(val))+")")
    print("~~~~~~~~~~~ End content ~~~~~~~~~~~")


def main():
    rpc = ServerProxy('http://localhost:'+sys.argv[1])
    print("Referencing server on port 8080...")
    commands = ["show","get <key number>","corrupt < key number>","terminate","quit"]
    while True:
        print("Commands:")
        for com in commands: print(" " + com)
        user_input = raw_input("> ").split(" ")
        choice  = -1
        for index , com in enumerate(commands):
            if user_input[0] == com.split(" ")[0]: choice = index
        try:
            if choice == 0: # show
                show_content(rpc)
            elif choice == 1: # get
                if len(user_input) != 2: raise ValueError
                key_num = user_input[1]
                print(repr(rpc.get(Binary(key_num)).data))
            elif choice == 2: # corrupt
                if len(user_input) != 2: raise ValueError
                key_num = user_input[1]
                rpc.corrupt(Binary(key_num))
                show_content(rpc)
            elif choice == 3: # terminate
                print("Terminating...")
                rpc.terminate()
            elif choice == 4: # quit
                break
            else: raise ValueError
        except socket.error:
            print("Server is down.")
        except ValueError:
            print("invalid command!")


if __name__ == "__main__":
    main()
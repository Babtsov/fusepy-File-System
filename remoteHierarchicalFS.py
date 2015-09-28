#!/usr/bin/env python
import os
import logging

from itertools import count
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
from pickle import dumps, loads
from xmlrpclib import Binary, ServerProxy

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

if not hasattr(__builtins__, 'bytes'):
    bytes = str

rpc = ServerProxy('http://localhost:8080')

class File(object):
    """ Represents a file (regular file, directory, or soft link) on the file system.
        PROPERTIES OF THE OBJECT:
        self.properties: contains all the attr and xattr the OS uses to categorize the files,
            this includes their type too (dict).
        self.file_type: returns the type of the file (directory, regular, or link)
        self.data:
            Directory file: self.data is a dict<name,serial number>
            Regular file: self.data contains the content of the file as str
            Link file: self.data contains a FULL path (from the OS root) stored as a str
    """
    def __str__(self): # function used for debugging
        representation = "[[File: name: {0}, data: {1}, ser num: {2} ]]".format(
            self.name, repr(self.data), self.serial_number)
        return representation
    _id = count(0)
    def __init__(self,absolute_path,properties,data):
        if absolute_path == '/': self.name = absolute_path
        else: self.name = os.path.basename(absolute_path)
        self.properties = properties
        self.data = data
        self.serial_number = self._id.next() #generate a unique serial number for every file

    @staticmethod
    def push(serial_num,file): # pushes a file to the rpc server and associates it with a serial number in ht
        rpc.put(Binary(str(serial_num)),Binary(dumps(file)),3000)

    @staticmethod
    def pull(serial_num): # returns the file that corresponds to the serial number
        try:
            response_dict = rpc.get(Binary(str(serial_num)))
            binary_value = response_dict["value"]
        except:
            raise FuseOSError(ENOENT)
        pickled_obj = binary_value.data
        file = loads(pickled_obj)
        if file == None: # File has been removed
            print "The file at node {0} has been removed.".format(serial_num)
            raise FuseOSError(ENOENT)
        return file

    file_type = property(lambda  self: self.properties['st_mode'] & 0770000)


class Memory(LoggingMixIn, Operations):
    def show_server_content(self): # function used for debugging
        print "**************** Server Contents ***************************"
        max_serial_num = 10
        for index in range(max_serial_num+1):
            try:
                file = loads(rpc.get(Binary(str(index)))['value'].data)
                if file == None: # File has been removed
                    print index,": ","The file at node {0} has been removed.".format(index)
                else:
                    print index,": ",File.pull(index)
            except:
                print index,": ","The file at node {0} doesn't exist.".format(index)
        print "************************************************************"

    def __init__(self):
        self.fd = 0
        now = time()
        root_properties = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)
        root = File('/',root_properties, {})
        File.push(root.serial_number,root) # store in the server ht serial number -> object

    @staticmethod
    def lut_lookup(path):
        root = File.pull(0) # first pull the root
        if path == '/':
            return root
        path_parts = path.split("/")[1:] # [1:] to get rid of the first element ''
        if path_parts[-1] == '': # path_parts[-1] will be '' if path ends with / (./a/b/)
            path_parts.pop() # remove it so we won't be iterating through an empty name
        context = root
        for name in path_parts:
            try:
                serial_num = context.data[name]
            except KeyError:
                raise FuseOSError(ENOENT)
            file = File.pull(serial_num)
            assert isinstance(file, File)
            if file.file_type == S_IFDIR: # we have a directory
                context = file # set the directory as the new context for the lookup
            else:
                return file # we have a regular file, so return it
        # if we reached this point, it means that the requested file is a dir, so return it
        return context

    # @staticmethod
    # def lut_update(file,**kwargs):
    #     file_lut = File.pull(0)
    #     if kwargs['action'] == 'add file':
    #         file_lut[file.absolute_path] = file.serial_number
    #     elif kwargs['action'] == 'remove file':
    #         del file_lut[file.absolute_path]
    #     else: raise RuntimeError
    #     File.push(0,file_lut)

    @staticmethod
    def ht_update(file,**kwargs): # update the hash table of the server
        if kwargs['action'] == 'add file' or kwargs['action'] == 'update file':
            File.push(file.serial_number,file)
        elif kwargs['action'] == 'remove file':
            File.push(file.serial_number,None) # mark file as deleted
        else: raise RuntimeError

    def chmod(self, path, mode):
        file = self.lut_lookup(path)
        file.properties['st_mode'] &= 0770000
        file.properties['st_mode'] |= mode
        self.ht_update(file,action='update file')
        return 0

    def chown(self, path, uid, gid):
        file = self.lut_lookup(path)
        file.properties['st_uid'] = uid
        file.properties['st_gid'] = gid
        self.ht_update(file,action='update file')

    def create(self, path, mode, fi=None):
        print "create(self, {0}, {1})".format(path,mode)
        # first make a new regular file, update server ht
        new_file_propeties = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
        new_file = File(path,new_file_propeties,bytes()) # make am empty file
        self.ht_update(new_file,action="add file")
        # then, pull the parent directory, add a reference to it and push it back
        parent_dir = self.lut_lookup(os.path.dirname(path))
        assert parent_dir.file_type == S_IFDIR
        parent_dir.data[new_file.name]=new_file.serial_number
        self.ht_update(parent_dir,action='update file')
        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        print "getattr(self, {0}, {1})".format(path,fh)
        self.show_server_content() # debug
        return self.lut_lookup(path).properties

    def getxattr(self, path, name, position=0):
        print "getxattr(self, {0}, {1}, {2})".format(path,name,position)
        attrs = self.lut_lookup(path).properties.get('attrs', {})
        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        print "listxattr(self, {0}".format(path)
        attrs = self.lut_lookup(path).properties.get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        print "mkdir(self, {0}, {1})".format(path,mode)
        # first make a new dir, and push it to the remote server
        new_dir_properties = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
        new_dir = File(path,new_dir_properties,{})
        self.ht_update(new_dir,action='add file')
        # then, pull the parent directory, add a reference to it and push it back
        parent_dir = self.lut_lookup(os.path.dirname(path))
        assert parent_dir.file_type == S_IFDIR
        parent_dir.data[new_dir.name]=new_dir.serial_number
        parent_dir.properties['st_nlink'] += 1
        self.ht_update(parent_dir,action='update file')

    def open(self, path, flags):
        print "open(self, {0}, {1})".format(path,flags)
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        print "read(self, {0}, {1}, {2}, {3})".format(path,size,offset,fh)
        file = self.lut_lookup(path)
        assert file.file_type == S_IFREG
        return file.data[offset:offset + size]

    def readdir(self, path, fh):
        print "readdir(self, {0}, {1})".format(path,fh)
        directory = self.lut_lookup(path)
        assert directory.file_type == S_IFDIR
        return ['.', '..'] + [x for x in directory.data]

    def readlink(self, path): #TODO:: symbolic links
        print "readlink(self, {0})".format(path)
        link = self.lookup(path)
        assert link.file_type == S_IFLNK
        return link.data

    def removexattr(self, path, name):
        print "removexattr(self, {0}, {1})".format(path,name)
        file = self.lut_lookup(path)
        attrs = file.properties.get('attrs', {})
        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR
        self.ht_update(file,action='update file')

    def rename(self, old, new):
        print "rename(self, {0}, {1})".format(old,new)
        file = self.lut_lookup(old)
        # pull old parent, remove reference, and push it back
        old_parent = self.lut_lookup(os.path.dirname(old))
        assert old_parent.file_type == S_IFDIR
        del old_parent.data[file.name]
        self.ht_update(old_parent,action='update file')
        # update the absolute_path property of the object, update lut, and push it back
        file.name = os.path.basename(new)
        self.ht_update(file,action='update file')
        # pull the new parent, add reference to it, and push it back
        new_parent = self.lut_lookup(os.path.dirname(new))
        assert new_parent.file_type == S_IFDIR
        new_parent.data[file.name] = file.serial_number
        self.ht_update(new_parent,action='update file')

    def rmdir(self, path):
        print "rmdir(self, {0})".format(path)
        dir, parent_dir = pull_file(path), pull_file(os.path.dirname(path))
        parent_dir.data.remove(path)
        parent_dir.properties['st_nlink'] -= 1
        push_file(os.path.dirname(path), parent_dir)
        push_file(path, None) # mark the old dir as removed in the server ht

    def setxattr(self, path, name, value, options, position=0):
        print "setxattr(self, {0}, {1}, {2}, {3}, {4})".format(path,name,value,options,position)
        # Ignore options
        file = pull_file(path)
        attrs = file.properties.setdefault('attrs', {})
        attrs[name] = value
        push_file(path,file)

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source): #TODO:: symbolic links
        print "symlink(self, {0}, {1})".format(target,source)
        link_properties = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,st_size=len(source),
                               st_ctime=time(), st_mtime=time(),st_atime=time())
        source_file = self.lookup(source)
        parent_dir = self.lookup(os.path.dirname(target))
        full_os_path = os.getcwd() + '/' + argv[1] + source_file.absolute_path
        link = File(target,link_properties,full_os_path)
        parent_dir.data[link.name] = link

    def truncate(self, path, length, fh=None):
        print "truncate(self, {0}, {1}, {2})".format(path,length,fh)
        file = self.lut_lookup(path)
        assert file.file_type == S_IFREG
        file.data = file.data[:length]
        file.properties['st_size'] = length
        self.ht_update(file,action='update file')

    def unlink(self, path):
        print "unlink(self, {0})".format(path)
        # remove reference from the parent dir
        file = self.lut_lookup(path)
        parent_dir = self.lut_lookup(os.path.dirname(path))
        assert parent_dir.file_type == S_IFDIR
        del parent_dir.data[file.name]
        self.ht_update(parent_dir,action='update file')
        # mark file as removed in the server ht
        self.ht_update(file,action='remove file')

    def utimens(self, path, times=None):
        print "utimens(self, {0}, {1})".format(path,times)
        now = time()
        atime, mtime = times if times else (now, now)
        file = self.lut_lookup(path)
        file.properties['st_atime'] = atime
        file.properties['st_mtime'] = mtime
        self.ht_update(file,action='update file')

    def write(self, path, data, offset, fh):
        print "write(self, {0}, {1}, {2}, {3})".format(path,data,offset,fh)
        file = self.lut_lookup(path)
        assert file.file_type == S_IFREG
        file.data = file.data[:offset] + data
        file.properties['st_size'] = len(file.data)
        self.ht_update(file,action='update file')
        return len(data)


if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(Memory(), argv[1], foreground=True, debug = False)
    # fusermount -uz ./fusemount


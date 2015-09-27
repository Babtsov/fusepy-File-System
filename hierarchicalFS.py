#!/usr/bin/env python
import os
import logging

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


if not hasattr(__builtins__, 'bytes'):
    bytes = str


class File(object):
    """ Represents a file (regular file, directory, or soft link) on the file system.
        self.name: the name of the file (str)
        self.context_name: the name of the directory that contains the file (str)
        self.properties: contains all the attr and xattr the OS uses to categorize the files,
            this includes their type too (dict).
        self data:
            Directory file: self.data is a dict <name,File>
            Regular file: self.data contains the content of the file as str
    """
    def __init__(self,absolute_path,properties,data):
        self.set_abs_path(absolute_path) # initialize self.name and self.context_name
        self.properties = properties
        self.data = data

    def get_type(self): # check if the file is a directory or a regular file
        return self.properties['st_mode'] & 0770000

    def get_abs_path(self): #get the absolute path
        if self.name == '/': return '/'
        return self.context_name + '/' + self.name

    def set_abs_path(self,absolute_path):
        if absolute_path == '/':
            self.name = '/'
            self.context_name = ''
        else:
            path_elements = absolute_path.split('/')
            self.name = path_elements.pop()
            self.context_name = '/'.join(path_elements)


class Memory(LoggingMixIn, Operations):

    def __init__(self):
        self.fd = 0
        now = time()
        root_properties = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)
        self.root = File('/', root_properties, {})

    def lookup(self,path):
        if path == '/':
            return self.root
        path_parts = path.split("/")[1:] # [1:] to get rid of the first element ''
        context = self.root
        for name in path_parts:
            try:
                file = context.data[name]
            except KeyError:
                raise FuseOSError(ENOENT)
            assert isinstance(file, File)
            if file.get_type() == S_IFDIR: # we have a directory
                context = file # set the directory as the new context for the lookup
            else:
                return file # we have a regular file, so return it
        # if we reached this point, it means that the requested file is a dir, so return it
        return context

    def chmod(self, path, mode):
        self.lookup(path).properties['st_mode'] &= 0770000
        self.lookup(path).properties['st_mode'] |= mode
        return 0

    def chown(self, path, uid, gid):
        self.lookup(path).properties['st_uid'] = uid
        self.lookup(path).properties['st_gid'] = gid

    def create(self, path, mode, fi=None):
        print "create(self, {0}, {1})".format(path,mode)
        new_file_propeties = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
        new_file = File(path,new_file_propeties,bytes()) # make am empty file
        parent_dir = self.lookup(os.path.dirname(path))
        assert parent_dir.get_type() == S_IFDIR
        parent_dir.data[new_file.name] = new_file # include a reference to the new file in the parent dir
        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        print "getattr(self, {0}, {1})".format(path,fh)
        return self.lookup(path).properties

    def getxattr(self, path, name, position=0):
        print "getxattr(self, {0}, {1}, {2})".format(path,name,position)
        attrs = self.lookup(path).properties.get('attrs', {})
        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):  # TODO:: verify
        print "listxattr(self, {0}".format(path)
        attrs = self.lookup(path).properties.get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        print "mkdir(self, {0}, {1})".format(path,mode)
        new_dir_properties = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
        new_dir = File(path,new_dir_properties,{})
        parent_dir = self.lookup(os.path.dirname(path))
        assert parent_dir.get_type() == S_IFDIR
        parent_dir.data[new_dir.name] = new_dir # include a reference to the new dir in the parent dir
        parent_dir.properties['st_nlink'] += 1

    def open(self, path, flags):
        print "open(self, {0}, {1})".format(path,flags)
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        print "read(self, {0}, {1}, {2}, {3})".format(path,size,offset,fh)
        file = self.lookup(path)
        assert file.get_type() == S_IFREG
        return file.data[offset:offset + size]

    def readdir(self, path, fh):
        print "readdir(self, {0}, {1})".format(path,fh)
        directory = self.lookup(path)
        assert directory.get_type() == S_IFDIR
        return ['.', '..'] + [x for x in directory.data]

    def readlink(self, path):  # TODO::
        print "readlink(self, {0})".format(path)
        return self.data[path]

    def removexattr(self, path, name):  # TODO:: verify
        print "removexattr(self, {0}, {1})".format(path,name)
        attrs = self.lookup(path).properties.get('attrs', {})
        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        print "rename(self, {0}, {1})".format(old,new)
        relocated_file = self.lookup(old)
        old_parent = self.lookup(os.path.dirname(old))
        del old_parent.data[relocated_file.name] # pop the File from the old location
        relocated_file.set_abs_path(new) # change the name and the context_name of the file.
        self.lookup(os.path.dirname(new)).data[relocated_file.name] = relocated_file

    def rmdir(self, path):  # TODO::
        self.files.pop(path)
        self.files['/']['st_nlink'] -= 1

    def setxattr(self, path, name, value, options, position=0):  # TODO:: verify
        print "setxattr(self, {0}, {1}, {2}, {3}, {4})".format(path,name,value,options,position)
        # Ignore options
        attrs = self.lookup(path).properties.setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):  # TODO::
        self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
                                  st_size=len(source))
        self.data[target] = source

    def truncate(self, path, length, fh=None):
        print "truncate(self, {0}, {1}, {2})".format(path,length,fh)
        file = self.lookup(path)
        assert file.get_type() == S_IFREG
        file.data = file.data[:length]
        file.properties['st_size'] = length

    def unlink(self, path):
        print "unlink(self, {0})".format(path)
        parent_dir = self.lookup(os.path.dirname(path))
        file = self.lookup(path)
        parent_dir.data.pop(file.name)

    def utimens(self, path, times=None):  # TODO:: verify
        print "utimens(self, {0}, {1})".format(path,times)
        now = time()
        atime, mtime = times if times else (now, now)
        file = self.lookup(path)
        file.properties['st_atime'] = atime
        file.properties['st_mtime'] = mtime

    def write(self, path, data, offset, fh):
        print "write(self, {0}, {1}, {2}, {3})".format(path,data,offset,fh)
        file = self.lookup(path)
        assert file.get_type() == S_IFREG
        file.data = file.data[:offset] + data
        file.properties['st_size'] = len(file.data)
        return len(data)


if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(Memory(), argv[1], foreground=True, debug = False)
    # fusermount -uz ./fusemount

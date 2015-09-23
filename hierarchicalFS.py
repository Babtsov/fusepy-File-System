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
    """  If file is a dir: data should be dict <str(name),object(File)>
         if file is a reg: data should contain the content as str
    """
    def __init__(self,absolute_path,properties,data):
        if absolute_path == '/':
            self.name = '/'
            self.context_name = ''
        else:
            path_elements = absolute_path.split('/')
            self.name = path_elements.pop()
            self.context_name = '/'.join(path_elements)
        self.properties = properties
        self.data = data

    def get_type(self): # check if the file is a directory or a regular file
        return self.properties['st_mode'] & 0770000

    def get_abs_path(self): #get the absolute path
        if self.name == '/': return '/'
        return self.context_name + '/' + self.name


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
            print "name: ",name
            print"context.data: ",context.data
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

    def chmod(self, path, mode): # TODO::
        self.files[path]['st_mode'] &= 0770000
        self.files[path]['st_mode'] |= mode
        return 0

    def chown(self, path, uid, gid): # TODO::
        self.files[path]['st_uid'] = uid
        self.files[path]['st_gid'] = gid

    def create(self, path, mode):
        print "create(self,   {0},   {1})".format(path,mode)
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
        print "getattr(self,   {0},   {1})".format(path,fh)
        file = self.lookup(path)
        print type(file)
        return file.properties

    def getxattr(self, path, name, position=0):
        print "getxattr(self,   {0},   {1}  ,{2})".format(path,name,position)
        file = self.lookup(path)
        attrs = file.properties.get('attrs', {})

        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):  # TODO::
        print "listxattr"
        attrs = self.files[path].get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):  # TODO:: verify
        print "mkdir(self,   {0},   {1})".format(path,mode)
        new_dir_properties = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
        new_dir = File(path,new_dir_properties,{})
        parent_dir = self.lookup(os.path.dirname(path))
        assert parent_dir.get_type() == S_IFDIR
        parent_dir.data[new_dir.name] = new_dir # include a reference to the new dir in the parent dir
        parent_dir.properties['st_nlink'] += 1

    def open(self, path, flags):
        print "open(self,   {0},   {1})".format(path,flags)
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):  # TODO:: verify
        print "read(self,   {0},   {1},   {2},   {3})".format(path,size,offset,fh)
        file = self.lookup(path)
        assert file.get_type() == S_IFREG
        return file.data[offset:offset + size]

    def readdir(self, path, fh):
        print "readdir(self,   {0},   {1})".format(path,fh)
        directory = self.lookup(path)
        assert directory.get_type() == S_IFDIR
        return ['.', '..'] + [x for x in directory.data]

    def readlink(self, path):  # TODO::
        print "readlink(self,   {0})".format(path)
        return self.data[path]

    def removexattr(self, path, name):  # TODO::
        print "removexattr(self,   {0},   {1})".format(path,name)
        # attrs = self.files[path].get('attrs', {})
        #
        # try:
        #     del attrs[name]
        # except KeyError:
        #     pass        # Should return ENOATTR

    def rename(self, old, new):  # TODO::
        self.files[new] = self.files.pop(old)

    def rmdir(self, path):  # TODO::
        self.files.pop(path)
        self.files['/']['st_nlink'] -= 1

    def setxattr(self, path, name, value, options, position=0):  # TODO::
        print "setxattr(self,   {0},   {1},   {2},   {3},   {4})".format(path,name,value,options,position)
        # Ignore options
        attrs = self.files[path].setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):  # TODO::
        self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
                                  st_size=len(source))
        self.data[target] = source

    def truncate(self, path, length, fh=None):  # TODO::
        self.data[path] = self.data[path][:length]
        self.files[path]['st_size'] = length

    def unlink(self, path):  # TODO::
        self.files.pop(path)

    def utimens(self, path, times=None):  # TODO::
        now = time()
        # atime, mtime = times if times else (now, now)
        # self.files[path]['st_atime'] = atime
        # self.files[path]['st_mtime'] = mtime

    def write(self, path, data, offset, fh):  # TODO::
        print "write(self,   {0},   {1},   {2},   {3})".format(path,data,offset,fh)
        self.data[path] = self.data[path][:offset] + data
        self.files[path]['st_size'] = len(self.data[path])
        return len(data)


if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(Memory(), argv[1], foreground=True, debug = False)
    # fusermount -uz ./fusemount

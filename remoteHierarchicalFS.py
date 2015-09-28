#!/usr/bin/env python
import os
import logging

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


def push_file(path,file):
    rpc.put(Binary(path),Binary(dumps(file)),3000)


def pull_file(path):
    try:
        binary_value, ttl = rpc.get(Binary(path)).values()
    except ValueError:
        raise FuseOSError(ENOENT)
    pickled_obj = binary_value.data
    if pickled_obj == None: # File has been removed
        print "The file at {0} has been removed.".format(path)
        raise FuseOSError(ENOENT)
    return loads(pickled_obj)


class File(object):
    """ Represents a file (regular file, directory, or soft link) on the file system.
        PROPERTIES OF THE OBJECT:
        self.absolute_path: the absolute path of the file.
            The following properties depend on the absolute_path and are read-only
            self.name: the name of the file (str)
            self.context_name: the name of the directory that contains the file (str)

        self.properties: contains all the attr and xattr the OS uses to categorize the files,
            this includes their type too (dict).

        self.data:
            Directory file: self.data is a list of absolute paths
            Regular file: self.data contains the content of the file as str
            Link file: self.data contains a FULL path (from the OS root) stored as a str
    """
    def __init__(self,absolute_path,properties,data):
        self.absolute_path = absolute_path
        self.properties = properties
        self.data = data

    def get_type(self): # check if the file is a directory or a regular file
        return self.properties['st_mode'] & 0770000

    name = property(lambda self: os.path.basename(self.absolute_path))
    context_name = property(lambda self: os.path.dirname(self.absolute_path))


class Memory(LoggingMixIn, Operations):
    def __init__(self):
        self.fd = 0
        now = time()
        root_properties = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)
        root = File('/', root_properties, [])
        push_file('/',root)

    def chmod(self, path, mode):
        file = pull_file(path)
        file.properties['st_mode'] &= 0770000
        file.properties['st_mode'] |= mode
        push_file(path,file)
        return 0

    def chown(self, path, uid, gid):
        file = pull_file(path)
        file.properties['st_uid'] = uid
        file.properties['st_gid'] = gid
        push_file(path,file)

    def create(self, path, mode, fi=None):
        print "create(self, {0}, {1})".format(path,mode)
        # first make a new regular file, and push it to the remote server
        new_file_propeties = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
        new_file = File(path,new_file_propeties,bytes()) # make am empty file
        push_file(path,new_file)
        # then, pull the parent directory, add a reference to it and push it back
        parent_dir = pull_file(os.path.dirname(path))
        assert parent_dir.get_type() == S_IFDIR
        parent_dir.data.append(path)
        push_file(os.path.dirname(path),parent_dir)
        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        print "getattr(self, {0}, {1})".format(path,fh)
        return pull_file(path).properties

    def getxattr(self, path, name, position=0):
        print "getxattr(self, {0}, {1}, {2})".format(path,name,position)
        attrs = pull_file(path).properties.get('attrs', {})
        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        print "listxattr(self, {0}".format(path)
        attrs = pull_file(path).properties.get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        print "mkdir(self, {0}, {1})".format(path,mode)
        # first make a new dir, and push it to the remote server
        new_dir_properties = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
        new_dir = File(path,new_dir_properties,[])
        push_file(path,new_dir)
        # then, pull the parent directory, add a reference to it and push it back
        parent_dir = pull_file(os.path.dirname(path))
        assert parent_dir.get_type() == S_IFDIR
        parent_dir.data.append(path)
        parent_dir.properties['st_nlink'] += 1
        push_file(os.path.dirname(path),parent_dir)


    def open(self, path, flags):
        print "open(self, {0}, {1})".format(path,flags)
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        print "read(self, {0}, {1}, {2}, {3})".format(path,size,offset,fh)
        file = pull_file(path)
        assert file.get_type() == S_IFREG
        return file.data[offset:offset + size]

    def readdir(self, path, fh):
        print "readdir(self, {0}, {1})".format(path,fh)
        directory = pull_file(path)
        assert directory.get_type() == S_IFDIR
        return ['.', '..'] + [os.path.basename(x) for x in directory.data]

    def readlink(self, path): #TODO:: symbolic links
        print "readlink(self, {0})".format(path)
        link = self.lookup(path)
        assert link.get_type() == S_IFLNK
        return link.data

    def removexattr(self, path, name):
        print "removexattr(self, {0}, {1})".format(path,name)
        attrs = pull_file(path).properties.get('attrs', {})
        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        print "rename(self, {0}, {1})".format(old,new)
        relocated_file = pull_file(old)
        old_parent = self.lookup(os.path.dirname(old))
        del old_parent.data[relocated_file.name] # pop the File from the old location
        relocated_file.absolute_path = new # change the name and the context_name of the file.
        self.lookup(os.path.dirname(new)).data[relocated_file.name] = relocated_file

    def rmdir(self, path):
        print "rmdir(self, {0})".format(path)
        dir, parent_dir = pull_file(path), pull_file(os.path.dirname(path))
        parent_dir.data.remove(path)
        parent_dir.properties['st_nlink'] -= 1
        push_file(os.path.dirname(path), parent_dir)
        push_file(path, None) # mark the old dir as removed

    def setxattr(self, path, name, value, options, position=0):
        print "setxattr(self, {0}, {1}, {2}, {3}, {4})".format(path,name,value,options,position)
        # Ignore options
        attrs = self.lookup(path).properties.setdefault('attrs', {})
        attrs[name] = value

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
        file = pull_file(path)
        assert file.get_type() == S_IFREG
        file.data = file.data[:length]
        file.properties['st_size'] = length
        push_file(path,file)

    def unlink(self, path):
        print "unlink(self, {0})".format(path)
        parent_dir = self.lookup(os.path.dirname(path))
        file = self.lookup(path)
        parent_dir.data.pop(file.name)

    def utimens(self, path, times=None):
        print "utimens(self, {0}, {1})".format(path,times)
        now = time()
        atime, mtime = times if times else (now, now)
        file = pull_file(path)
        file.properties['st_atime'] = atime
        file.properties['st_mtime'] = mtime
        push_file(path,file)

    def write(self, path, data, offset, fh):
        print "write(self, {0}, {1}, {2}, {3})".format(path,data,offset,fh)
        file = pull_file(path)
        assert file.get_type() == S_IFREG
        file.data = file.data[:offset] + data
        file.properties['st_size'] = len(file.data)
        push_file(path,file)
        return len(data)


if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(Memory(), argv[1], foreground=True, debug = False)
    # fusermount -uz ./fusemount


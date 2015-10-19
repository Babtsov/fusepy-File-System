#!/usr/bin/env python
#
# To start MongoDB: mongod --port 27027
# To mount FS:      python RemoteDB_FS.py fusemount 27027 233
# To unmount FS:    fusermount -uz ./fusemount

import os
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
from remote_services import FileStorageManager
from fuse import FUSE, FuseOSError, Operations


if not hasattr(__builtins__, 'bytes'):
    bytes = str


class ClientFS(Operations):

    def __init__(self,storage_manager):
        self.fd = 0
        self.storage = storage_manager

    def chmod(self, path, mode):
        print "chmod(self, {0}, {1})".format(path,mode)
        file_dict = self.storage.lookup(path)
        file_dict['meta']['st_mode'] &= 0770000
        file_dict['meta']['st_mode'] |= mode
        self.storage.update_file(file_dict,'meta',file_dict['meta'])
        return 0

    def chown(self, path, uid, gid):
        print "chown(self, {0}, {1}, {2})".format(path,uid,gid)
        file_dict = self.storage.lookup(path)
        file_dict['meta']['st_uid'] = uid
        file_dict['meta']['st_gid'] = gid
        self.storage.update_file(file_dict,'meta',file_dict['meta'])

    def create(self, path, mode, fi=None):
        print "create(self, {0}, {1})".format(path,mode)
        now = time()
        file_meta = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=now, st_mtime=now,
                                st_atime=now)
        file_dict = dict(name=os.path.basename(path),meta=file_meta,type='reg',data='')
        self.storage.insert_file(file_dict)
        parent_dict = self.storage.lookup(os.path.dirname(path))
        self.storage.update_dir_data(parent_dict,action='$add',child_dict=file_dict)
        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        print "getattr(self, {0}, {1})".format(path,fh)
        file_dict = self.storage.lookup(path)
        meta = self.storage.lookup(path)['meta']
        self.storage.db.print_db()
        self.storage.cache.print_cache()
        return meta

    def getxattr(self, path, name, position=0):
        print "getxattr(self, {0}, {1}, {2})".format(path,name,position)
        attrs = self.storage.lookup(path)['meta'].get('attrs', {})
        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        print "listxattr(self, {0}".format(path)
        attrs = self.storage.lookup(path).get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        print "mkdir(self, {0}, {1})".format(path,mode)
        now = time()
        dir_meta = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=now, st_mtime=now,
                                st_atime=now)
        dir_dict = dict(name=os.path.basename(path),meta=dir_meta,type='dir',data={})
        self.storage.insert_file(dir_dict)
        parent_dict = self.storage.lookup(os.path.dirname(path))
        self.storage.update_dir_data(parent_dict,action='$add',child_dict=dir_dict)
        parent_dict['meta']['st_nlink'] += 1
        self.storage.update_file(parent_dict,'meta',parent_dict['meta'])

    def open(self, path, flags):
        print "open(self, {0}, {1})".format(path,flags)
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        print "read(self, {0}, {1}, {2}, {3})".format(path,size,offset,fh)
        file_dict = self.storage.lookup(path)
        return file_dict['data'][offset:offset + size]

    def readdir(self, path, fh):
        print "readdir(self, {0}, {1})".format(path,fh)
        dir_dict = self.storage.lookup(path)
        assert dir_dict['type'] == 'dir'
        return ['.', '..'] + [x for x in dir_dict['data']]

    def readlink(self, path):
        print "readlink(self, {0})".format(path)
        link_dict = self.storage.lookup(path)
        assert link_dict['type'] == 'link'
        return link_dict['data']

    def removexattr(self, path, name):
        print "removexattr(self, {0}, {1})".format(path,name)
        file_dict = self.storage.lookup(path)
        attrs = file_dict['meta'].get('attrs', {})
        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR
        self.storage.update_file(file_dict,'meta',file_dict['meta'])

    def rename(self, old, new):
        print "rename(self, {0}, {1})".format(old,new)
        file_dict = self.storage.lookup(old)
        # print 'file dict renamed is: ',file_dict
        old_parent_dict = self.storage.lookup(os.path.dirname(old))
        self.storage.update_dir_data(old_parent_dict,action='$remove',child_name=file_dict['name'])
        file_dict['name'] = os.path.basename(new)
        new_parent_dict = self.storage.lookup(os.path.dirname(new))
        self.storage.update_dir_data(new_parent_dict,action='$add',child_dict=file_dict)
        
    def rmdir(self, path):
        print "rmdir(self, {0})".format(path)
        file_dict = self.storage.lookup(path)
        parent_dict = self.storage.lookup(os.path.dirname(path))
        parent_dict['meta']['st_nlink'] -= 1
        self.storage.update_file(parent_dict,'meta',parent_dict['meta'])
        self.storage.update_dir_data(parent_dict, action='$remove', child_name=file_dict['name'])
        self.storage.remove_file(file_dict)

    def setxattr(self, path, name, value, options, position=0):
        print "setxattr(self, {0}, {1}, {2}, {3}, {4})".format(path,name,value,options,position)
        # Ignore options
        file_dict = self.storage.lookup(path)
        attrs = file_dict['meta'].setdefault('attrs', {})
        attrs[name] = value
        self.storage.update_file(parent_dict,'meta',file_dict['meta'])

    def statfs(self, path):
        print "statfs(self, {0})".format(path)
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        print "symlink(self, {0}, {1})".format(target,source)
        now = time()
        link_meta = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,st_size=len(source),
                                  st_ctime=now, st_mtime=now,st_atime=now)
        file_system_os_path = os.getcwd() + '/' + argv[1] # the path of the FUSE FS relative to the OS's FS
        source_path = source
        if file_system_os_path in source:
            source_path = source.replace(file_system_os_path,'')
        full_os_path = os.getcwd() + '/' + argv[1] + source_path
        link_dict = dict(name=os.path.basename(target),meta=link_meta,type='link',data=full_os_path)
        self.storage.insert_file(link_dict)
        parent_dict = self.storage.lookup(os.path.dirname(target))
        self.storage.update_dir_data(parent_dict,action='$add',child_dict=link_dict)

    def truncate(self, path, length, fh=None):
        print "truncate(self, {0}, {1}, {2})".format(path,length,fh)
        file_dict = self.storage.lookup(path)
        assert file_dict['type'] == 'reg'
        file_dict['data'] = file_dict['data'][:length]
        file_dict['meta']['st_size'] = length
        self.storage.update_file(file_dict,'meta',file_dict['meta'])

    def unlink(self, path):
        print "unlink(self, {0})".format(path)
        file_dict = self.storage.lookup(path)
        parent_dict = self.storage.lookup(os.path.dirname(path))
        self.storage.update_dir_data(parent_dict,action='$remove',child_name=file_dict['name'])
        self.storage.remove_file(file_dict)

    def utimens(self, path, times=None):
        print "utimens(self, {0}, {1})".format(path,times)
        now = time()
        atime, mtime = times if times else (now, now)
        file_dict = self.storage.lookup(path)
        file_dict['meta']['st_atime'] = atime
        file_dict['meta']['st_atime'] = mtime
        self.storage.update_file(file_dict,'meta',file_dict['meta'])

    def write(self, path, data, offset, fh):
        print "write(self, {0}, {1}, {2}, {3})".format(path,data,offset,fh)
        file_dict = self.storage.lookup(path)
        assert file_dict['type'] == 'reg'
        file_dict['data'] = file_dict['data'][:offset] + data
        self.storage.update_file(file_dict,'data',file_dict['data'])
        file_dict['meta']['st_size'] = len(file_dict['data'])
        self.storage.update_file(file_dict,'meta',file_dict['meta'])
        return len(data)


if __name__ == '__main__':
    print "argv: ",argv
    if len(argv) != 4:
        print('usage: %s <mountpoint> <port number> <cache size>' % argv[0])
        exit(1)
    mount_point, port_num, cache_size = argv[1:]
    try:
        port_num, cache_size = int(port_num), int(cache_size)
    except TypeError:
        print('usage: %s <mountpoint> <port number> <cache size>' % argv[0])
        exit(1)
    print "CLEARING THE DATABASE"                           # ~~~DEBUG
    from pymongo import  MongoClient                        # ~~~DEBUG
    MongoClient('localhost',27027).FS_DB.FUSEPY_FS.drop()   # ~~~DEBUG
    fuse = FUSE(ClientFS(FileStorageManager('localhost',int(port_num),cache_size)), argv[1], foreground=True, debug = False)

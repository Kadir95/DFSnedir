#!/usr/bin/env python

from __future__ import with_statement

import os
import sys
import errno
import rpyc

from fuse import FUSE, FuseOSError, Operations

class Passthrough(Operations):
    def __init__(self):
        self.conn = rpyc.connect("172.20.0.2", 11223)

    # Helpers
    # =======

    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path

    # Filesystem methods
    # ==================

    def access(self, path, mode):
        print("FileSystem method: access\n")
        full_path = self._full_path(path)
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    #chmod
    #chown

    def getattr(self, path, fh=None):
        print("FileSystem method: getattr\n")
        full_path = self._full_path(path)
        st = os.lstat(full_path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                     'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    def readdir(self, path, fh):
        print("FileSystem method: readdir\n")
        full_path = self._full_path(path)

        dirents = ['.', '..']
        if os.path.isdir(full_path):
            dirents.extend(os.listdir(full_path))
        for r in dirents:
            yield r

    #readlink
    #mknod

    def rmdir(self, path):
        print("FileSystem method: rmdir\n")
        full_path = self._full_path(path)
        return os.rmdir(full_path)

    def mkdir(self, path, mode):
        print("FileSystem method: mkdir\n")
        return os.mkdir(self._full_path(path), mode)

    #statfs
    #unlink
    #symlink

    def rename(self, old, new):
        print("FileSystem method: rename\n")
        return os.rename(self._full_path(old), self._full_path(new))

    #link
    #utimens

    # File methods
    # ============

    def open(self, path, flags):
        print("File method: open\n")
        full_path = self._full_path(path)
        return os.open(full_path, flags)

    def create(self, path, mode, fi=None):
        print("File method: creat\n")
        full_path = self._full_path(path)
        return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        print("File method: read\n")
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

    def write(self, path, buf, offset, fh):
        print("File method: write\n")
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)

    #truncate

    def flush(self, path, fh):
        print("File method: flush\n")
        return os.fsync(fh)

    #release
    #fsync

def mount(mountpoint):
    abs_path = os.path.abspath(mountpoint)
    if not os.path.isdir(abs_path):
        print("Mount point must be exist")
        sys.exit(0)
    if len(os.listdir(abs_path)) > 0:
        print("Mount point must be empty")
        sys.exit(0)
    
    FUSE(Passthrough(), mountpoint, nothreads=True, foreground=True)

def usage():,
    print(sys.argv[0], "<mount point>")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        usage()
        sys.exit(0)
    
    mount(sys.argv[1])
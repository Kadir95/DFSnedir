#!/usr/bin/env python

from __future__ import with_statement

import os
import sys
import errno
import rpyc
import ctypes

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
        print("FileSystem method: access")
        r = self.conn.root.access(path, mode)
        return 0 # :(

    chmod = None
    chown = None

    def getattr(self, path, fh=None):
        print("FileSystem method: getattr")
        return self.conn.root.getattr(path, fh)

    def readdir(self, path, fh):
        print("FileSystem method: readdir")
        return self.conn.root.readdir(path, fh)

    readlink = None
    mknod = None

    def rmdir(self, path):
        print("FileSystem method: rmdir", path)
        return self.conn.root.rmdir(path)

    def mkdir(self, path, mode):
        print("FileSystem method: mkdir")
        return self.conn.root.mkdir(path, mode)

    def statfs(self):
        return None
    unlink = None
    symlink = None
    rename = None
    link = None
    utimens = None

    # File methods
    # ============

    def open(self, path, flags, mode=None):
        print("File method: open")
        return self.conn.root.open(path, flags, mode)

    def create(self, path, mode, fi=None):
        print("File method: creat")
        return self.conn.root.open(path, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        print("File method: read")
        return self.conn.root.read(path, length, offset, fh)

    def write(self, path, buf, offset, fh):
        print("File method: write")
        return self.conn.root.write(path, buf, offset, fh)

    truncate = None

    def flush(self, path, fh):
        print("File method: flush")
        return self.conn.root.flush(path, fh)

    def release(self, path, fh):
        print("File method: release")
        return self.conn.root.flush(path, fh)
    
    fsync = None 

def mount(mountpoint):
    abs_path = os.path.abspath(mountpoint)
    if not os.path.isdir(abs_path):
        print("Mount point must be exist")
        sys.exit(0)
    if len(os.listdir(abs_path)) > 0:
        print("Mount point must be empty")
        sys.exit(0)
    
    FUSE(Passthrough(), mountpoint, nothreads=True, foreground=True, nonempty=True)

def usage():
    print(sys.argv[0], "<mount point>")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        usage()
        sys.exit(0)
    
    mount(sys.argv[1])
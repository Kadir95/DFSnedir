"""
    DFSnedir_service class contains the functions that provided by containers RPyC server.

    !! Attention: If you want to reach the function by RPC call, you have to put "exposed_" prefix to your function
        Ex: exposed_echo -> conn.root.echo("some arguments") // It will run
            echo         -> conn.root.echo("some arguments") // It won't run
"""

import rpyc
import os
import pickle

path = "/data"

class DFSnedir_service(rpyc.Service):
    def _abs_path(self, fpath):
        if fpath.startswith("/"):
            fpath = fpath[1:]
        return os.path.join(path, fpath)

    def exposed_echo(self, text):
        return text + "\nFrom docker"
    
    def exposed_execute(self, a, b, op):
        return op(a, b)
    
    def exposed_access(self, full_path, mode):
        return os.access(path + full_path, mode)

    def exposed_chmod(self, full_path, mode):
        return os.chmod(path + full_path, mode)
    
    def exposed_chown(full_path, uid, gid):
        return os.chown(path + full_path, uid, gid)

    def exposed_lstat(self, full_path):
        return os.lstat(path + full_path)

    def exposed_path_isdir(self, full_path):
        return os.path.isdir(path + full_path)
    
    def exposed_listdir(self, full_path):
        return os.listdir(path + full_path)

    def exposed_readlink(self, full_path):
        return os.readlink(path + full_path)

    def exposed_path_relpath(self, pathname, root):
        return os.path.relpath(pathname, path)
    
    def exposed_mknod(self, full_path, mode, dev):
        return os.mknod(path + full_path, mode, dev)

    def exposed_rmdir(self, full_path):
        return os.rmdir(path + full_path)

    def exposed_mkdir(self, full_path, mode):
        return os.mkdir(path + full_path, mode)

    def exposed_statvfs(self, full_path):
        return os.statvfs(path + full_path)

    def exposed_unlink(self, full_path):
        return os.unlink(self._abs_path(full_path))
    
    def exposed_symlink(self, name, target):
        return os.symlink(nam)
    
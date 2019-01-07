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
        return text + " //" + os.environ["HOSTNAME"]
    
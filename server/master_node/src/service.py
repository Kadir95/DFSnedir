"""
    DFSnedir_service class contains the functions that provided by containers RPyC server.

    !! Attention: If you want to reach the function by RPC call, you have to put "exposed_" prefix to your function
        Ex: exposed_echo -> conn.root.echo("some arguments") // It will run
            echo         -> conn.root.echo("some arguments") // It won't run
"""

import rpyc
import os
import pickle


class DFSnedir_master_service(rpyc.Service):
    def on_connect(self, conn):
        self.slave_server_table_file = conn._config["slave_dict"]
        if os.path.getsize(self.slave_server_table_file) > 0:
            fd = open(self.slave_server_table_file, "rb")
            self.slave_server_table = pickle.load(fd)
            fd.close()
        else: 
            self.slave_server_table = {}

    def _abs_path(self, fpath):
        if fpath.startswith("/"):
            fpath = fpath[1:]
        return os.path.join(path, fpath)

    def exposed_echo(self, text):
        return text + "\nFrom docker"
    
    def exposed_add_slave(self, slave_stats):
        self.slave_server_table[slave_stats["ip"]] = slave_stats
        fd = open(self.slave_server_table_file, "wb")
        pickle.dump(self.slave_server_table, fd)
        fd.close()
    
    def exposed_get_slaves(self):
        return self.slave_server_table

    
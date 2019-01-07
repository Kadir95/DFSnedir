"""
    DFSnedir_service class contains the functions that provided by containers RPyC server.

    !! Attention: If you want to reach the function by RPC call, you have to put "exposed_" prefix to your function
        Ex: exposed_echo -> conn.root.echo("some arguments") // It will run
            echo         -> conn.root.echo("some arguments") // It won't run
"""

import rpyc
import os
import pickle
import time

class DFSnedir_master_service(rpyc.Service):
    def _refresh_slave_dict(self):
        if os.path.getsize(self.slave_server_table_file) > 0:
            fd = open(self.slave_server_table_file, "rb")
            self.slave_server_table = pickle.load(fd)
            fd.close()
        else: 
            self.slave_server_table = {}

    def _flush_slave_dict(self):
        fd = open(self.slave_server_table_file, "wb")
        pickle.dump(self.slave_server_table, fd)
        fd.close()

    def _abs_path(self, fpath):
        if fpath.startswith("/"):
            fpath = fpath[1:]
        return os.path.join(path, fpath)

    def on_connect(self, conn):
        self.slave_server_table_file = conn._config["slave_dict"]
        self._refresh_slave_dict()

    def exposed_echo(self, text):
        return str(text) + " //From docker"
    
    def exposed_add_slave(self, slave_stats):
        self._refresh_slave_dict()
        self.slave_server_table[slave_stats["id"]] = {"stats":slave_stats, "last_heart_beat":time.time()}
        self._flush_slave_dict()
    
    def exposed_get_slaves(self):
        self._refresh_slave_dict()
        return str(self.slave_server_table)

    def exposed_heart_beat(self, slave_stats):
        self._refresh_slave_dict()
        self.slave_server_table[slave_stats["id"]]["last_heart_beat"] = time.time()
        self._flush_slave_dict()
    
    def exposed_slave_echo(self, text):
        self._refresh_slave_dict()
        slaves_list = self.slave_server_table.values()
        result_text = ""
        for slave in slaves_list:
            conn = rpyc.connect(slave["stats"]["ip"], slave["stats"]["port"])
            result_text += conn.root.echo(text) + "\n"
        return result_text

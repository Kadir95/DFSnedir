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
import random
import pprint

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

	def _refresh_file_dict(self):
		if os.path.getsize(self.file_server_table_file) > 0:
			fd = open(self.file_server_table_file, "rb")
			self.file_server_table = pickle.load(fd)
			fd.close()
		else:
			self.file_server_table = {}

	def _flush_file_dict(self):
		fd = open(self.file_server_table_file, "wb")
		pickle.dump(self.file_server_table, fd)
		fd.close()

	def _abs_path(self, fpath):
		raise NotImplemented

	def _find_slave(self, path):
		self._refresh_file_dict()
		slave_machine_list = self.file_server_table.get(path)
		if not slave_machine_list:
			self.file_server_table[path] = [self._select_rand_slaveID()]
			slave_machine_list = self.file_server_table.get(path)
			self._flush_file_dict()
		slaveID = slave_machine_list[0]
		slave_machine = self.slave_server_table[slaveID]
		# slave_machine["stats"]["ip"], slave_machine["stats"]["port"]
		return slave_machine

	def _select_rand_slaveID(self):
		slaves = self.slave_server_table.keys()
		return random.choice(list(slaves))

	def _slave_connect(self, slave):
		return rpyc.connect(slave["stats"]["ip"], slave["stats"]["port"])


	def on_connect(self, conn):
		self.slave_server_table_file = conn._config["slave_dict"]
		self.file_server_table_file = conn._config["file_dict"]
		self._refresh_slave_dict()
		self._refresh_file_dict()


	def exposed_echo(self, text):
		return str(text) + " //From docker"

	def exposed_add_slave(self, slave_stats):
		self._refresh_slave_dict()
		self.slave_server_table[slave_stats["id"]] = {"stats":slave_stats, "last_heart_beat":time.time()}
		self._flush_slave_dict()

	def exposed_get_slaves(self):
		self._refresh_slave_dict()
		return pprint.pformat(self.slave_server_table)

	def exposed_get_file_distribution(self):
		self._refresh_file_dict()
		return pprint.pformat(self.file_server_table)

	def exposed_heart_beat(self, slave_stats):
		self._refresh_slave_dict()
		self.slave_server_table[slave_stats["id"]]["last_heart_beat"] = time.time()
		self._flush_slave_dict()

	def exposed_slave_echo(self, text):
		self._refresh_slave_dict()
		slaves_list = self.slave_server_table.values()
		result_text = ""
		for slave in slaves_list:
			conn = self._slave_connect(slave)
			result_text += conn.root.echo(text) + "\n"
		return result_text

	def exposed_unlink(self, path):
		slave = self._find_slave(path)
		conn = self._slave_connect(slave)
		return conn.root.unlink(path)

	def exposed_access(self, path, mode):
		slave = self._find_slave(path)
		conn = self._slave_connect(slave)
		return conn.root.access(path, mode)

	def exposed_rmdir(self, path):
		slave = self._find_slave(path)
		conn = self._slave_connect(slave)
		return conn.root.rmdir(path)

	def exposed_mkdir(self, path, mode):
		slaveID = self._select_rand_slaveID()
		self.file_server_table[path] = [slaveID]
		slave = self.slave_server_table[slaveID]
		conn = self._slave_connect(slave)
		self._flush_file_dict()
		return conn.root.mkdir(path, mode)

	def exposed_readdir(self, path, fh):
		self._refresh_slave_dict()
		slaves = self.slave_server_table.values()
		directory = ['.', '..']
		for slave in slaves:
			conn = self._slave_connect(slave)
			content = conn.root.readdir(path, fh)
			for c in content:
				if c not in directory:
					directory.append(c)
			#directory.append(c for c in content if c not in directory)
		return directory

	def exposed_getattr(self, path, fh=None):
		slave = self._find_slave(path)
		conn = self._slave_connect(slave)
		return conn.root.getattr(path, fh)

	def exposed_open(self, path, flags, mode):
		slave = self._find_slave(path)
		conn = self._slave_connect(slave)
		return conn.root.open(path, flags, mode)

	def exposed_read(self, path, length, offset, fh):
		slave = self._find_slave(path)
		conn = self._slave_connect(slave)
		return conn.root.read(path, length, offset, fh)

	def exposed_write(self, path, buf, offset, fh):
		slave = self._find_slave(path)
		conn = self._slave_connect(slave)
		return conn.root.write(path, buf, offset, fh)

	def exposed_truncate(self, path, length, fh=None):
		slave = self._find_slave(path)
		conn = self._slave_connect(slave)
		return conn.root.truncate(path, length, fh)


	def exposed_flush(self, path, fh):
		slave = self._find_slave(path)
		conn = self._slave_connect(slave)
		return conn.root.flush(path, fh)
	
	def exposed_close(self, path, fh):
		slave = self._find_slave(path)
		conn = self._slave_connect(slave)
		return conn.root.flush(path, fh)
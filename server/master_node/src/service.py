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
		slave_machine_list = self.file_server_table[path]
		slaveID = slave_machine_list[0]
		slave_machine = self.slave_server_table[slaveID]
		# slave_machine["stats"]["ip"], slave_machine["stats"]["port"]
		return slave_machine

	def _select_rand_slaveID(self):
		slaves = self.slave_server_table.keys()
		return random.choice(slaves)

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

	# def exposed_access(self, path, mode):
	# 	self._refresh_file_dict()
	# 	slave_machine_list = self.file_server_table[path]
	# 	slaveID = slave_machine_list[0]
	# 	slave_machine = self.slave_server_table[slaveID]
	# 	conn = rpyc.connect(slave_machine["stats"]["ip"], slave_machine["stats"]["port"])
	# 	return conn.root.access(path, mode)

	def exposed_access(self, path, mode):
		slave = self._find_slave(path)
		conn = rpyc.connect(slave["stats"]["ip"], slave["stats"]["port"])
		return conn.root.access(path, mode)

	def exposed_rmdir(self, path):
		slave = self._find_slave()
		conn = rpyc.connect(slave["stats"]["ip"], slave["stats"]["port"])
		return conn.root.rmdir(path)

	def exposed_mkdir(self, path, mode):
		slaveID = self._select_rand_slaveID()
		self.file_server_table[path] = [slaveID]
		slave = self.slave_server_table[slaveID]
		conn = rpyc.connect(slave["stats"]["ip"], slave["stats"]["port"])
		self._flush_file_dict()
		return conn.root.mkdir(path, mode)

	def exposed_readdir(self, path, fh):
		self._refresh_slave_dict()
		slaves = self.slave_server_table.values()
		directory = []
		for slave in slaves:
			conn = rpyc.connect(slave["stats"]["ip"], slave["stats"]["port"])
			content = conn.root.readdir(path)
			directory.append(c for c in content if c not in directory)
		return directory

	def exposed_getattr(self, path, fh=None):
		slave = self._find_slave(path)
		conn = rpyc.connect(slave["stats"]["ip"], slave["stats"]["port"])
		st = conn.root.lstat(path)
		return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
														'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size',
														'st_uid'))

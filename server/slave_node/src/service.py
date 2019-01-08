"""
	DFSnedir_service class contains the functions that provided by containers RPyC server.

	!! Attention: If you want to reach the function by RPC call, you have to put "exposed_" prefix to your function
		Ex: exposed_echo -> conn.root.echo("some arguments") // It will run
			echo         -> conn.root.echo("some arguments") // It won't run
"""

import rpyc
import os
import pickle

d_path = "/data/slave_data/"

class DFSnedir_service(rpyc.Service):
	def _abs_path(self, u_path):
		if u_path.startswith("/"):
			u_path = u_path[1:]
		return os.path.join(d_path, u_path)

	def on_connect(self, conn):
		if not os.path.isdir(d_path):
			os.makedirs(d_path)

	def exposed_echo(self, text):
		return text + " //" + os.environ["HOSTNAME"]

	def exposed_open(self, path, flags, mode):
		# if a slave closes when file is reading. It will be an error (fh file descriptor just exist on the slave not all!)
		a_path = self._abs_path(path)
		return os.open(a_path, flags)
	
	def exposed_read(self, path, length, offset, fh):
		# if a slave closes when file is reading. It will be an error (fh file descriptor just exist on the slave not all!)
		os.lseek(fh, offset, os.SEEK_SET)
		return os.read(fh, length)
	
	def exposed_write(self, path, buf, offset, fh):
		# if a slave closes when file is reading. It will be an error (fh file descriptor just exist on the slave not all!)
		os.lseek(fh, offset, os.SEEK_SET)
		return os.write(fh, buf)

	def exposed_flush(self, path, fh):
		# if a slave closes when file is reading. It will be an error (fh file descriptor just exist on the slave not all!)
		return os.close(fh)
	
	def exposed_access(self, path, mode):
		a_path = self._abs_path(path)
		return os.access(a_path, mode)

	def exposed_rmdir(self, path):
		a_path = self._abs_path(path)
		return os.rmdir(a_path)

	def exposed_mkdir(self, path, mode):
		a_path = self._abs_path(path)
		return os.mkdir(a_path, mode)

	def exposed_readdir(self, path, fh):
		# Master node collect all list from all slaves that holds different files on the directory and reduce them ('.' and '..' will be added by master node)
		a_path = self._abs_path(path)
		return os.listdir(a_path)

	def exposed_getattr(self, path, fh=None):
		a_path = self._abs_path(path)
		stats = os.lstat(a_path)
		return dict((key, getattr(stats, key)) for key in ('st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))


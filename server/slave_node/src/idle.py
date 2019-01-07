import time
import os
import socket
import rpyc
from multiprocessing import Process
from rpyc.utils.server import ThreadedServer
import service

def test():
    port = int(os.environ["TESTPORT"])
    host = os.environ["HOSTNAME"]

    print("Serve on ip:%s, port:%d" %(host, port))

    _socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _socket.bind((host, port))
    _socket.listen(5)

    while True:
        conn, addr = _socket.accept()
        print("Get connection from", addr)
        conn.send(b'Received your connection')
        conn.close()

def foo():
    port = int(os.environ["PORT"])
    rypc_server = ThreadedServer(service.DFSnedir_service, port=port)
    rypc_server.start()

if __name__ == "__main__":
    ptest = Process(target=test)
    ptest2 = Process(target=foo)
    ptest.start()
    ptest2.start()
    ptest.join()
    ptest2.join()
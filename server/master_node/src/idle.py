import time
import os
import socket
import rpyc
import pickle
from multiprocessing import Process
from rpyc.utils.server import ThreadedServer
import service

rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True

slave_dict = "/data/slave_dict"

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

def heart_beat_controller():
    pass

def foo():
    port = int(os.environ["PORT"])
    rypc_server = ThreadedServer(
            service.DFSnedir_master_service,
            port=port,
            protocol_config={
                    "slave_dict":slave_dict,
                    "allow_pickle":True
                    })
    rypc_server.start()

if __name__ == "__main__":

    if not os.path.isdir(os.path.dirname(slave_dict)):
        os.makedirs(os.path.dirname(slave_dict))

    if not os.path.isfile(slave_dict):
        temp = {}
        fd = open(slave_dict, "wb")
        pickle.dump(temp, fd)
        fd.close()

    ptest = Process(target=test)
    ptest2 = Process(target=foo)
    ptest.start()
    ptest2.start()
    ptest.join()
    ptest2.join()
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
    interval = int(os.environ["HEART_BEAT_SEC"]) * 2
    while True:
        time.sleep(interval / 2)
        curr_time = time.time()
        fd = open(slave_dict, "rb")
        s_dict = pickle.load(fd)
        s_list = s_dict.values()
        fd.close()

        del_list = []
        for s_ in s_list:
            timeout = curr_time - s_["last_heart_beat"]
            if timeout > interval:
                del_list.append(s_["stats"]["id"])
        
        if len(del_list) > 0:
            for i in del_list:
                s_dict.pop(i, None)
            fd = open(slave_dict, "wb")
            pickle.dump(s_dict, fd)
            fd.close()

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

    test_port_p = Process(target=test)
    rpyc_p = Process(target=foo)
    master_heart_beat_con_p = Process(target=heart_beat_controller)

    test_port_p.start()
    rpyc_p.start()
    master_heart_beat_con_p.start()

    test_port_p.join()
    rpyc_p.join()
    master_heart_beat_con_p.join()
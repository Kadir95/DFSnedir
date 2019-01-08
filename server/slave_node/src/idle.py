import time
import os
import socket
import rpyc
from multiprocessing import Process
from rpyc.utils.server import ThreadedServer
import service
import rpyc

rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True

master_node_conf = {
        "host":"172.20.0.2",
        "port":11223
}

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

def heart_beat(conn):
    interval = int(os.environ["HEART_BEAT_SEC"])
    while(True):
        time.sleep(interval)
        conn.root.heart_beat(get_slave_stats())

def connect_master():
    conn = None
    while conn == None:
        try:
            conn = rpyc.connect(master_node_conf["host"], master_node_conf["port"])
        except Exception as e:
            print("Master node cannot reachable Error:", e)
            conn = None
    conn.root.add_slave(get_slave_stats())
    heart_beat(conn)

def get_slave_stats():
    return {
            "id":os.environ["HOSTNAME"],
            "ip":socket.gethostbyname(socket.gethostname()),
            "port":int(os.environ["PORT"])
    }

def foo():
    port = int(os.environ["PORT"])
    rypc_server = ThreadedServer(service.DFSnedir_service, port=port, protocol_config={
                    "allow_pickle":True,
                    "allow_public_attrs":True
                    })
    rypc_server.start()

if __name__ == "__main__":
    test_port_p = Process(target=test)
    rpyc_p = Process(target=foo)
    master_node_keeper = Process(target=connect_master)
    
    test_port_p.start()
    master_node_keeper.start()
    rpyc_p.start()
    
    test_port_p.join()
    rpyc_p.join()
    master_node_keeper.join()
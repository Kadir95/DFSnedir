import time
import os
import socket

def main():
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

if __name__ == "__main__":
    main()
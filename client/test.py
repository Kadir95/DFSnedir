import rpyc

rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True


conn = rpyc.connect("172.20.0.2", 11223)

conn.root.add_slave({"ip":"aysenaz"})

print(conn.root.get_slaves())
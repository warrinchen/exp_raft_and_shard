from rpc import Rpc
import time

# 设置socket消息的本机地址和端口
rpc = Rpc(('localhost', 10002))
data, addr = rpc.recv(timeout=20)

print(data)
print(1)
time.sleep(0.1)

rpc.send('aaaa', ('127.0.0.1', 10000))

print(2)
rpc.close()

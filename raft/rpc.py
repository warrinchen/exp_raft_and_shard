import json
import socket


class Rpc(object):
    def __init__(self, addr=None, timeout=None):
        self.addr = None

        # 创建UDP socket
        self.ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # 地址不为空
        if addr:
            self.bind(tuple(addr))
        # 超时时间不为空
        if timeout:
            self.ss.settimeout(timeout)

    # 设置消息的发送地址（主机名、端口号）
    def bind(self, addr):
        self.addr = tuple(addr)
        self.ss.bind(addr)

    # 设置阻塞套接字操作的超时时间
    def set_timeout(self, timeout):
        self.ss.settimeout(timeout)

    # 发送UDP消息
    def send(self, data, addr):
        data = json.dumps(data).encode("utf-8")
        self.ss.sendto(data, tuple(addr))

    def recv(self, addr=None, timeout=None):
        if addr:
            self.bind(addr)
        if not self.addr:
            raise ("please bind to an addr")

        if timeout:
            self.set_timeout(timeout)

        # 接收UDP消息
        data, addr = self.ss.recvfrom(65535)
        # 解析消息内容和地址
        return json.loads(data), addr

    # 关闭消息
    def close(self):
        self.ss.close()

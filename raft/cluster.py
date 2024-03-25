import collections

# 可以使用关键字参数和位置参数初始化namedtuple
Node = collections.namedtuple('Node', ['id', 'ip', 'port'])
# 组中节点的数量
CLUSTER_SIZE = 7
# 心跳最大超时时间
TIMEOUT_MAX = 10
# 心跳时间间隔
HEART_BEAT_INTERVAL = float(TIMEOUT_MAX / 3)

# 所有服务器的基本配置信息, 包括id, port_id, uri
class Cluster:
    # 端口号
    ids = range(0, CLUSTER_SIZE)
    # 每个节点的地址和端口
    ip = "localhost"
    ports = [10000 + n for n in ids]
    uris = [f'localhost:1000{n}' for n in ids]

    def __init__(self):
        # 得到组中所有节点的id和地址的数组
        self._nodes = [Node(nid, self.ip, port) for nid, port in enumerate(self.ports, start=0)]

    def __len__(self):
        # 组中节点的数量
        return len(self._nodes)

    def __getitem__(self, index):
        return self._nodes[index]

    def __repr__(self):
        # 设置print的输出格式
        return ", ".join([f'{n.id}@{n.ip}@{n.port}' for n in self._nodes])


if __name__ == '__main__':
    cluster = Cluster()
    print(cluster[0])
    print(cluster)

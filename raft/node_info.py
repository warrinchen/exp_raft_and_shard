from raft.cluster import Cluster
from raft.rpc import Rpc


# 初始化节点信息
class NodeInfo:
    def __init__(self, followers, current_term=0, log=[], leader=None, node=None):
        # 组信息
        self.cluster = Cluster()
        # 节点信息
        self.node = node
        # 节点id
        self.id = node.id

        # 当前term值(主要为选举的正常进行)
        self.current_term = current_term
        # 日志信息，格式为(term, log)
        self.log = log
        # 组中leader信息: [id, (ip, port)]
        self.leader = leader

        # 设置socket消息的本机地址和端口
        self.rpc = Rpc((node.ip, node.port))
        # 组中的其他节点
        # self.followers = [peer for peer in self.cluster if peer != self.node]
        self.followers = followers


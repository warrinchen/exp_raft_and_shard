import logging
import random
import time
import collections
from raft.cluster import Cluster, TIMEOUT_MAX
from raft.rpc import Rpc

logging.basicConfig(level=logging.INFO)
# 操作记录格式
Operate = collections.namedtuple('Operate', ['state', 'operate', 'timeout'])


class Client(object):
    def __init__(self, client_id, client_name, client_ip, client_port):
        # 组信息
        self.cluster = Cluster()

        # 客户id
        self.id = client_id
        # 客户地址
        self.ip = client_ip
        self.port = client_port
        # 客户名称
        self.node = client_name
        # 当前操作
        self.current_operate = 0
        # 操作集
        self.operates = []
        # 已提交操作集
        self.commit_operates = []
        # 操作超时时间
        self.operate_timeout = TIMEOUT_MAX

        # 设置socket消息的本机地址和端口
        self.rpc = Rpc((client_ip, client_port))
        # 组的leader
        self.leader_id = None

    # 执行操作(接收输入)
    def set_operate(self):
        time.sleep(0.1)
        # 接收用户输入
        self.current_operate = input("请输入一个操作数（从0按顺序输入数字）：")
        # 加入操作集
        self.operates.append([False, self.current_operate, time.time() + self.operate_timeout])

    # 发送操作
    def send_operate(self, operate=None):
        # 发送操作消息
        if operate is None:
            operate = self.current_operate
        operation = {
            'type': 'Operation',
            'clientId': self.id,
            'operate': operate,
            'ip': self.ip,
            'port': self.port
        }
        # 向组中leader发送消息
        if self.leader_id is not None:
            leader = [peer for peer in self.cluster if peer.id == self.leader_id][0]
            print(f'client{self.id} 向leader{self.leader_id} 发送操作信息')
            self.rpc.send(operation, (leader.ip, leader.port))
        # 向组中一个server发送消息
        else:
            nodes = self.cluster
            node = nodes[random.randint(0, len(nodes) - 1)]
            print(f'client{self.id} 向node{node.id} 发送操作信息')
            self.rpc.send(operation, (node.ip, node.port))

    def run(self):
        print('开始接收leader返回的消息')
        while True:
            try:
                # 接收消息
                data, addr = self.rpc.recv()
                # 判断消息类型
                if data is not None:
                    # 收到leader的回复
                    if data['type'] == 'Operation_Response':
                        print("收到leader的操作提交消息")
                        # 修改leader提交的操作集的状态
                        self.operates[int(data['operate'])][0] = True
                        # 客户端操作集为
                        print(f'客户短的操作集为：{self.operates}')
                        # 添加到已提交操作集
                        self.commit_operates.append(data['operate'])
                        print(f'已提交的操作集为：{self.commit_operates}')
                        # 设置leader
                        self.leader_id = data['leaderId']
            except Exception:
                data, _ = None, None

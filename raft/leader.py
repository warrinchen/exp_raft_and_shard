import logging
import time
import collections

from raft.cluster import HEART_BEAT_INTERVAL
from raft.node import Node

logging.basicConfig(level=logging.INFO)
# 日志记录格式
Log = collections.namedtuple('Log', ['term', 'log'])


class Leader(Node):
    def __init__(self, node):
        super(Leader, self).__init__(node)
        # 设置节点状态
        self.node_state = 'leader'
        # leader是否过期
        self.overdue = False
        # 设置组中的leader信息
        self.leader = [self.id, (self.node.ip, self.node.port)]

        # 设置下一个日志位置索引
        self.next_index = 1

        # 日志记录紧跟nextIndex的上一个索引
        self.prev_index = -1
        # 日志记录紧跟nextIndex的上一个term值和日志记录
        if self.prev_index == -1:
            self.prev_term = 0
            self.prev_log = None
        else:
            self.prev_term = self.log[self.prev_index].term
            self.prev_log = self.log[self.prev_index].log

        # 向follower更新的日志条目
        self.entries = []

        # 要提交的最后一个日志条目
        self.commit_index = 0
        # 是否提交
        self.state = True

        # 已同步的节点
        self.commit_nodes = []
        self.commit_nodes.append(self.id)

        # 客户端地址
        self.client_ip = None
        self.client_port = None

        # 设置下次心跳时间
        self.next_heart_time = time.time() + HEART_BEAT_INTERVAL

    # 初始心跳
    def initial_heartbeat(self):
        # 不断发送心跳
        logging.info(f'leader{self.node.id} 向组中其他节点发送初始心跳信息，以终止正在进行的选举')
        # 发送心跳消息
        Heart = {
            'type': 'Init_AppendEntries',
            'leaderId': self.id,
            'term': self.current_term
        }
        # 向组中其他server发送消息
        for peer in self.followers:
            self.rpc.send(Heart, (peer.ip, peer.port))

    # 心跳
    def heartbeat(self):
        logging.info(f'leader{self.node.id} 向组中其他节点发送心跳信息')
        logging.info(f'leader{self.node.id} 的日志为:{self.log}')
        # 发送心跳消息
        Heart = {
            'type': 'AppendEntries',
            'leaderId': self.id,
            'term': self.current_term,
            'prevLogIndex': self.prev_index,
            'prevLogTerm': self.prev_term,
            'prevLog': self.prev_log,
            'entries': self.entries,
            'commitIndex': self.commit_index
        }
        # 向组中其他server发送消息
        for peer in self.followers:
            self.rpc.send(Heart, (peer.ip, peer.port))
        # 重置下次心跳时间
        self.next_heart_time = time.time() + HEART_BEAT_INTERVAL

    def run(self):
        while True:
            try:
                # 接收消息
                data, addr = self.rpc.recv()
                # 判断消息类型
                if data is not None:
                    # 收到其他candidate投票请求
                    if data['type'] == 'RequestVote':
                        logging.info("收到candidate{} 的投票请求".format(data['candidateId']))
                        # 请求的term大于当前leader的term
                        if data['term'] > self.current_term:
                            logging.info(f'candidate的term:{data["term"]} > 当前节点的term:{self.current_term} ，此leader已过期')
                            self.overdue = True
                    # 收到其他leader的心跳
                    if data['type'] == 'AppendEntries':
                        logging.info("收到leader{} 的心跳消息".format(data['leaderId']))
                        # 请求的term大于当前leader的term
                        if data['term'] > self.current_term:
                            logging.info(f'心跳的term:{data["term"]} > 当前节点的term:{self.current_term} ，此leader已过期')
                            self.overdue = True
                    # 收到follower的心跳回应
                    if data['type'] == 'AppendEntries_Response':
                        # 日志不同步
                        if not data['success']:
                            logging.info(
                                f'node:{data["id"]}日志不同步，当前比较位置:{data["prevLogIndex"]} ，此leader的日志长度为{len(self.log)}')
                            # 得到日志记录的上一个索引位置
                            prevLogIndex = data['prevLogIndex'] - 1
                            # 日志记录的上一个term值
                            if prevLogIndex == -1:
                                prevLogTerm = 0
                                prevLog = None
                            else:
                                prevLogTerm = self.log[prevLogIndex].term
                                prevLog = self.log[self.prev_index].log
                            # 将上次的日志记录添加到entries
                            entries = []
                            # 同步日志记录（将本地日志从当前索引位置后替换为entries）
                            for i in range(0, len(data['entries'])):
                                log = Log(data['entries'][i][0], data['entries'][i][1])
                                entries.append(log)
                            entries.insert(0, self.log[data['prevLogIndex']])
                            # 发送同步消息
                            response = {
                                'type': 'AppendEntries',
                                'leaderId': self.id,
                                'term': self.current_term,
                                'prevLogIndex': prevLogIndex,
                                'prevLogTerm': prevLogTerm,
                                'prevLog': prevLog,
                                'entries': entries,
                                'commitIndex': self.commit_index
                            }
                            # 同步消息（在发送消息前需要先停止一下，否则可能无法发送该消息）
                            time.sleep(0.01)
                            self.rpc.send(response, addr)
                        else:
                            # 日志已同步
                            logging.info(f'node{data["id"]}日志已同步，此leader的日志长度为{len(self.log)}')
                            if len(data['entries']) != 0:
                                logging.info(f'存在正在进行日志同步的操作:{self.log[self.commit_index].log}')
                                # 对日志已同步节点进行计数
                                self.commit_nodes.append(data['id'])
                                self.commit_nodes = list(set(self.commit_nodes))
                                logging.info(
                                    '当前日志同步情况:{}, 需要:{}'.format(len(self.commit_nodes),
                                                                (int(len(self.cluster) / 2)) + 1))
                                if len(self.commit_nodes) > int(len(self.cluster) / 2):
                                    # 超过半数完成，提交该操作
                                    logging.info(f'操作:{self.log[self.commit_index].log} 的日志已同步，提交该操作')
                                    # 将提交消息返回给客户端
                                    # 日志提交消息
                                    response = {
                                        'type': 'Operation_Response',
                                        'leaderId': self.id,
                                        'term': self.current_term,
                                        'operate': self.log[self.commit_index].log,
                                        'commitIndex': self.commit_index
                                    }
                                    # 同步消息（在发送消息前需要先停止一下，否则可能无法发送该消息）
                                    time.sleep(0.01)
                                    self.rpc.send(response, (self.client_ip, self.client_port))
                                    # 判断下一个日志索引位置是否还存在日志记录
                                    if self.next_index >= len(self.log):
                                        # 不存在下一个日志记录
                                        self.state = True
                                        # 设置下一个日志位置索引
                                        self.next_index = self.next_index + 1
                                        # 应该处理的日志索引位置
                                        self.commit_index = self.commit_index + 1
                                        # 日志记录上一个索引
                                        self.prev_index = self.commit_index - 1
                                        # 日志记录紧跟nextIndex的上一个term值和日志记录
                                        if self.prev_index == -1:
                                            self.prev_term = 0
                                            self.prev_log = None
                                        else:
                                            self.prev_term = self.log[self.prev_index].term
                                            self.prev_log = self.log[self.prev_index].log
                                        # 清空entries
                                        self.entries = []
                                    elif self.next_index < len(self.log):
                                        self.commit_nodes = [self.id]
                                        # 设置下一个日志位置索引
                                        self.next_index = self.next_index + 1
                                        # 应该处理的日志索引位置
                                        self.commit_index = self.commit_index + 1
                                        # 日志记录上一个索引
                                        self.prev_index = self.commit_index - 1
                                        # 日志记录紧跟nextIndex的上一个term值和日志记录
                                        if self.prev_index == -1:
                                            self.prev_term = 0
                                            self.prev_log = None
                                        else:
                                            self.prev_term = self.log[self.prev_index].term
                                            self.prev_log = self.log[self.prev_index].log

                                        # 设置要同步的日志
                                        self.entries = [self.log[self.commit_index]]
                                        # 消息
                                        response = {
                                            'type': 'AppendEntries',
                                            'leaderId': self.id,
                                            'term': self.current_term,
                                            'prevLogIndex': self.prev_index,
                                            'prevLogTerm': self.prev_term,
                                            'prevLog': self.prev_log,
                                            'entries': self.entries,
                                            'commitIndex': self.commit_index
                                        }
                                        time.sleep(0.01)
                                        # 向组中其他server发送消息
                                        for peer in self.followers:
                                            self.rpc.send(response, (peer.ip, peer.port))
                                        # 重置下次心跳时间
                                        self.next_heart_time = time.time() + HEART_BEAT_INTERVAL
                    # 收到client操作请求
                    if data['type'] == 'Operation':
                        logging.info("收到client{} 的操作请求:{}".format(data['clientId'], data['operate']))
                        # 客户端操作
                        operate = data['operate']
                        # 客户端地址
                        self.client_ip = data['ip']
                        self.client_port = data['port']
                        # 将操作添加到日志记录中
                        self.log.append(Log(self.current_term, operate))
                        # 当前日志记录是否提交
                        if self.state:
                            logging.info("当前日志已提交，同步该操作请求")
                            self.state = False
                            self.commit_nodes = [self.id]
                            # 设置要同步的日志
                            self.entries = [self.log[self.commit_index]]
                            # 消息
                            response = {
                                'type': 'AppendEntries',
                                'leaderId': self.id,
                                'term': self.current_term,
                                'prevLogIndex': self.prev_index,
                                'prevLogTerm': self.prev_term,
                                'prevLog': self.prev_log,
                                'entries': self.entries,
                                'commitIndex': self.commit_index
                            }
                            time.sleep(0.01)
                            # 向组中其他server发送消息
                            for peer in self.followers:
                                self.rpc.send(response, (peer.ip, peer.port))
                            # 重置下次心跳时间
                            self.next_heart_time = time.time() + HEART_BEAT_INTERVAL
            except Exception:
                data, _ = None, None

    def __repr__(self):
        return f'{type(self).__name__, self.node.id, self.current_term}'

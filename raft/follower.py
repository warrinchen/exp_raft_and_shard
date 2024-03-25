import logging
import time
import collections
from random import randrange

from raft.cluster import TIMEOUT_MAX
from raft.node import Node

logging.basicConfig(level=logging.INFO)
# 日志记录格式
Log = collections.namedtuple('Log', ['term', 'log'])


class Follower(Node):
    def __init__(self, node):
        super(Follower, self).__init__(node)
        # 设置节点状态
        self.node_state = 'follower'
        # 设置心跳超时时间
        self.heart_timeout = float(randrange(TIMEOUT_MAX / 2, TIMEOUT_MAX))
        # 下次心跳超时的时间
        self.next_heart_timeout = time.time() + self.heart_timeout

    def run(self):
        while True:
            try:
                # 接收消息
                data, addr = self.rpc.recv()
                # 判断消息类型
                if data is not None:
                    # 收到其他candidate投票请求，进行投票
                    if data['type'] == 'RequestVote':
                        logging.info("收到candidate{} 的投票请求".format(data['candidateId']))
                        vote_result = self.vote(data)
                        # 成功投票
                        if vote_result.vote_granted:
                            # 将自己的当前term设置为candidate的term
                            self.current_term = vote_result.term
                        # 响应消息
                        response = {
                            'type': 'RequestVote_Response',
                            'id': self.id,
                            'term': self.current_term,
                            'vote_granted': vote_result.vote_granted
                        }
                        # 发送投票消息（在发送消息前需要先停止一下，否则可能无法发送该消息）
                        time.sleep(0.01)
                        self.rpc.send(response, addr)
                    # 收到leader的初始心跳
                    elif data['type'] == 'Init_AppendEntries':
                        # leader的term大于等于当前节点的term
                        if data['term'] >= self.current_term:
                            # 设置组的leader id
                            self.leader = (data['leaderId'], addr)
                            # 将自己的当前term设置为leader的term
                            self.current_term = data['term']
                            # 重置下次心跳超时的时间
                            self.next_heart_timeout = time.time() + self.heart_timeout
                            logging.info("收到leader{} 的初始心跳消息，重置超时时间".format(data['leaderId']))
                    # 收到leader的心跳
                    elif data['type'] == 'AppendEntries':
                        # leader的term大于等于当前节点的term
                        if data['term'] >= self.current_term:
                            # 设置组的leader信息
                            self.leader = (data['leaderId'], addr)
                            # 将自己的当前term设置为leader的term
                            self.current_term = data['term']
                            # 重置下次心跳超时的时间
                            self.next_heart_timeout = time.time() + self.heart_timeout
                            logging.info("收到leader{} 的心跳消息".format(data['leaderId']))
                            logging.info(f'follower{self.node.id} 的日志为:{self.log}')
                            # TODO 进行日志同步
                            # 判断leader的上一个日志记录是否与本节点的日志记录相同
                            # 若本节点的日志记录在leader日志索引位置为空
                            if data['prevLogIndex'] >= len(self.log):
                                logging.info(
                                    f"本follower日志不同步，当前日志比较位置为:{data['prevLogIndex']}，本节点日志总长度为:{len(self.log)}")
                                # 响应消息(日志索引位置不同步)
                                response = {
                                    'type': 'AppendEntries_Response',
                                    'id': self.id,
                                    'term': self.current_term,
                                    'prevLogIndex': data['prevLogIndex'],
                                    'entries': data['entries'],
                                    'success': False
                                }
                            # 若leader日志索引位置为-1，本地日志为空，或者本节点的日志记录在leader日志索引位置的term和log一致
                            elif data['prevLogIndex'] == -1 or (
                                    data['prevLogTerm'] == self.log[data['prevLogIndex']].term and data['prevLog'] ==
                                    self.log[data['prevLogIndex']].log):
                                logging.info(
                                    f"本follower找到日志已同步位置索引，索引位置为:{data['prevLogIndex']}")
                                # 响应消息（日志索引位置已同步）
                                response = {
                                    'type': 'AppendEntries_Response',
                                    'id': self.id,
                                    'term': self.current_term,
                                    'prevLogIndex': data['prevLogIndex'],
                                    'entries': data['entries'],
                                    'success': True
                                }
                                # 同步日志
                                # entries不为空
                                if len(data['entries']) != 0:
                                    entries = []
                                    # 同步日志记录（将本地日志从当前索引位置后替换为entries）
                                    for i in range(0, len(data['entries'])):
                                        log = Log(data['entries'][i][0], data['entries'][i][1])
                                        entries.append(log)
                                    self.log = self.log[:data['prevLogIndex'] + 1] + entries
                                    logging.info(f"本follower进行日志同步，同步后日志长度为:{len(self.log)}")
                                else:
                                    logging.info(f"本follower日志已同步，日志长度为:{len(self.log)}")
                            else:
                                logging.info(
                                    f"本follower日志不同步，当前日志比较位置为:{data['prevLogIndex']}，本节点日志总长度为:{len(self.log)}")
                                # 响应消息(日志索引位置不同步)
                                response = {
                                    'type': 'AppendEntries_Response',
                                    'id': self.id,
                                    'term': self.current_term,
                                    'prevLogIndex': data['prevLogIndex'],
                                    'entries': data['entries'],
                                    'success': False
                                }
                            # 响应心跳消息（在发送消息前需要先停止一下，否则可能无法发送该消息）
                            time.sleep(0.01)
                            self.rpc.send(response, addr)
                    # 收到客户端请求
                    elif data['type'] == 'Operation':
                        # 转发给leader
                        time.sleep(0.01)
                        self.rpc.send(data, self.leader[1])
            except Exception:
                data, _ = None, None

    def __repr__(self):
        return f'{type(self).__name__, self.node.id, self.current_term}'

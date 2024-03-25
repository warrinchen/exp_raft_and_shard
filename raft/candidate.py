import logging
import time
from random import randrange

from raft.cluster import TIMEOUT_MAX
from raft.node import Node

logging.basicConfig(level=logging.INFO)


class Candidate(Node):
    def __init__(self, node):
        super(Candidate, self).__init__(node)
        # 设置节点状态
        self.node_state = 'candidate'

        # 获取的选票
        self.votes = []
        # candidate给自己投票
        self.vote_for = self.id

        # 是否转变为follower
        self.lose = False

        # 设置选举超时时间（每一轮选举都重新生成，避免出现随机数一样而一直死锁的情况）
        self.election_timeout = float(randrange(TIMEOUT_MAX / 2, TIMEOUT_MAX))
        # 下次选举超时的时间
        self.next_election_timeout = time.time() + self.election_timeout

    # 进行选举
    def elect(self):
        # candidate的term自增1
        self.current_term = self.current_term + 1
        logging.info(f'candidate{self.node.id} 向组中其他节点发送选举请求')
        # 将自己节点信息添加到votes中
        self.votes.append(self.node)
        # 给组中每个节点发送选举请求
        RequestVote = {
            'type': 'RequestVote',
            'candidateId': self.id,
            'term': self.current_term,
            'lastLogIndex': self.last_log_index,
            'lastLogTerm': self.last_log_term
        }
        # 向组中其他server发送消息
        for peer in self.followers:
            self.rpc.send(RequestVote, (peer.ip, peer.port))

    def run(self):
        while True:
            try:
                # 接收消息
                data, addr = self.rpc.recv()
                # 判断消息类型
                if data is not None:
                    # 收到其他节点的投票
                    if data['type'] == 'RequestVote_Response':
                        # 判断follower是否投票
                        if data['vote_granted']:
                            # 获得的选票数加一
                            self.votes.append(data['id'])
                            logging.info('当前投票情况:{}, 需要:{}'.format(len(self.votes), (int(len(self.cluster) / 2)) + 1))
                    # 收到其他candidate投票请求，进行投票
                    if data['type'] == 'RequestVote':
                        logging.info("收到candidate{} 的投票请求".format(data['candidateId']))
                        vote_result = self.vote(data)
                        # 若投票成功
                        if vote_result.vote_granted:
                            # 将自己的term置为请求的term
                            self.current_term = data['term']
                            # 判断是否为请求的term > 当前candidate的term
                            if vote_result.type == 1:
                                # 请求term > 当前term，则退出candidate，回到follower状态
                                self.lose = True
                                logging.info("收到candidate{} 的term更大，退出到follower状态".format(data['candidateId']))
                            else:
                                # 重置下次选举超时的时间（避免出现投票消息还在路上，而此节点超时进入下一轮选举）
                                self.next_election_timeout = time.time() + self.election_timeout
                                logging.info("收到candidate{} 的term与当前candidate的相等，重置选举超时时间".format(data['candidateId']))
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
                    # 收到leader的心跳
                    if data['type'] == 'AppendEntries' or data['type'] == 'Init_AppendEntries':
                        # leader的term大于等于当前节点的term
                        if data['term'] >= self.current_term:
                            logging.info("收到leader{} 的心跳消息，退出candidate状态".format(data['leaderId']))
                            # 将自己的当前term设置为leader的term
                            self.current_term = data['term']
                            # 设置组的leader id
                            self.leader = (data['leaderId'], addr)
                            # 退出选举
                            self.lose = True
            except Exception:
                data, _ = None, None

    def win(self):
        # 是否赢得了选举
        return len(self.votes) > int(len(self.cluster) / 2)

    def __repr__(self):
        return f'{type(self).__name__, self.node.id, self.current_term}'

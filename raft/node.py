import collections
import logging
from raft.cluster import Cluster
from raft.rpc import Rpc

# 返回的数据格式
VoteResult = collections.namedtuple('VoteResult', ['vote_granted', 'type', 'term', 'id'])


# 记录节点上一个状态的信息
class Node:
    def __init__(self, node=None):
        # 组信息
        self.cluster = Cluster()
        # 节点信息
        self.node = node.node
        # 节点id
        self.id = node.id

        # 当前term值(主要为选举的正常进行)
        self.current_term = node.current_term
        # 日志信息，格式为(term, log)
        self.log = node.log
        # 组中leader信息
        self.leader = node.leader

        # 给那个candidate投票
        self.vote_for = None

        # 设置socket消息的本机地址和端口
        # self.rpc = Rpc((node.ip, node.port))
        self.rpc = node.rpc
        # 组中的其他节点
        self.followers = node.followers

        # 得到日志的最后一个term
        if len(self.log) == 0:
            self.last_log_term = 0
        else:
            self.last_log_term = self.log[-1].term
        # 得到日志的最后一个索引
        self.last_log_index = len(self.log)


    # 各状态节点的共有操作
    # 投票操作（输入请求投票的信息）
    def vote(self, vote_request):
        # 请求的term记录
        term = vote_request['term']
        # 请求candidate的编号
        candidate_id = vote_request['candidateId']
        # 请求日志的最后一个term
        last_log_term = vote_request['lastLogTerm']
        # 请求日志的最后一个索引
        last_log_index = vote_request['lastLogIndex']
        # 请求的term大于此节点的term
        if term > self.current_term:
            logging.info(f'candidate的term:{term} > 当前节点的term:{self.current_term} ，同意投票')
            # 记录自己给哪个candidate投票了
            self.vote_for = candidate_id
            # 进行投票
            return VoteResult(True, 1, self.current_term, self.id)
        # 请求的term小于此节点的term
        if term < self.current_term:
            logging.info(f'candidate的term:{term} < 当前节点的term:{self.current_term} ，拒绝投票')
            # 拒绝投票
            return VoteResult(False, -1, self.current_term, self.id)
        # 请求的term和此节点的term相同，并且该节点在此term周期内没有向其他节点投票，并且候选人的日志至少和接收者的一样完整
        if term == self.current_term and (self.vote_for is None or self.vote_for == candidate_id) and (
                (last_log_term > self.last_log_term) or (
                last_log_term == self.last_log_term and last_log_index >= self.last_log_index)):
            # 记录自己给哪个candidate投票了
            self.vote_for = candidate_id
            # 进行投票
            return VoteResult(True, 0, self.current_term, self.id)
        # 该节点在此term已经向其他节点投票，拒绝此candidate的请求
        logging.info(f'当前节点在此轮term已经向candidate:{self.vote_for} 投票，拒绝向candidate:{candidate_id} 投票')
        return VoteResult(False, -1, self.current_term, self.id)

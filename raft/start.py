import ctypes
import inspect
import logging
import threading
import time

from raft.candidate import Candidate
from raft.cluster import Cluster
from raft.follower import Follower
from raft.leader import Leader
from raft.node_info import NodeInfo

cluster = Cluster()
# 设置日志的打印等级
logging.basicConfig(level=logging.INFO)


class Start(object):
    def __init__(self, node_id, followers, current_term, log, leader):
        # 节点的id和地址
        self.node = cluster[node_id]
        # 初始化节点信息
        self.node = NodeInfo(followers, current_term, log, leader, self.node)
        # 设置节点初始状态
        self.node = Follower(self.node)

    def become_leader(self):
        logging.info(f'node{self.node.id} 当选为leader，向组中其他节点发送心跳消息')
        # 节点状态转变为leader
        self.node = Leader(self.node)
        # 发送初始心跳
        self.node.initial_heartbeat()
        # 等待candidate终止
        time.sleep(0.1)
        # 开启新的线程来接收消息
        t1 = threading.Thread(target=self.node.run)
        t1.start()
        # 发送一次心跳进行日志同步
        self.node.heartbeat()
        while True:
            # 判断是否需要进行心跳
            if time.time() >= self.node.next_heart_time:
                # 发出心跳
                self.node.heartbeat()
            time.sleep(0.1)

    def become_candidate(self):
        # 心跳超时，转变为candidate
        self.node = Candidate(self.node)
        logging.info(f'node{self.node.id} 成为candidate，选举超时时间为 {self.node.election_timeout} s')
        # 发起选举
        self.node.elect()
        # 开始接收消息
        self.node.run()

    def become_follower(self):
        # 转变为follower
        self.node = Follower(self.node)
        # 打印心跳超时时间
        logging.info(f'node{self.node.id} 成为follower，心跳超时时间为 {self.node.heart_timeout} s')
        # 开始接收消息
        self.node.run()

    def run(self):
        # 节点启动时首先是成为follower，开启新的线程来执行方法
        # todo: 根据标志判断是成为ld or fl
        t = threading.Thread(target=self.become_follower)
        t.start()
        while True:
            # 判断当前节点状态
            if self.node.node_state == 'follower':
                # 判断心跳是否超时
                if time.time() >= self.node.next_heart_timeout:
                    # 终止当前线程
                    self.stop_thread(t)
                    # 新线程将节点状态转变为candidate
                    t = threading.Thread(target=self.become_candidate)
                    t.start()
            if self.node.node_state == 'candidate':
                # 判断是否赢得了选举
                if self.node.win():
                    # 终止当前线程
                    self.stop_thread(t)
                    # 新线程将节点状态转变为leader
                    t = threading.Thread(target=self.become_leader)
                    t.start()
                # 判断是否选举失败
                elif self.node.lose:
                    # 终止当前线程
                    self.stop_thread(t)
                    # 新线程将节点状态转变为follower
                    t = threading.Thread(target=self.become_follower)
                    t.start()
                # 判断选举是否超时
                elif time.time() >= self.node.next_election_timeout:
                    # 终止当前线程
                    self.stop_thread(t)
                    # 新线程将节点状态转变为candidate
                    t = threading.Thread(target=self.become_candidate)
                    t.start()
            if self.node.node_state == 'leader':
                # 判断leader是否过期
                if self.node.overdue:
                    # 终止当前线程
                    self.stop_thread(t)
                    # 新线程将节点状态转变为follower
                    t = threading.Thread(target=self.become_follower)
                    t.start()
            # 等待时间不能过短，否则可能消息还未发送就终止了线程
            time.sleep(0.1)

    # 退出线程方法
    def _async_raise(self, tid, exctype):

        tid = ctypes.c_long(tid)

        if not inspect.isclass(exctype):
            exctype = type(exctype)

        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))

        if res == 0:

            raise ValueError("invalid thread id")

        elif res != 1:

            # """if it returns a number greater than one, you're in trouble,

            # and you should call it again with exc=NULL to revert the effect"""

            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)

            raise SystemError("PyThreadState_SetAsyncExc failed")

    def stop_thread(self, thread):
        self._async_raise(thread.ident, SystemExit)

    def __repr__(self):
        return f'{type(self).__name__, self.node}'

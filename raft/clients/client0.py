import threading
import time

from raft.client import Client


def send():

    while True:
        client.set_operate()
        client.send_operate()


if __name__ == '__main__':
    client = Client(0, 'client1', 'localhost', 11000)
    # 开启新的线程去监听leader返回的消息
    t1 = threading.Thread(target=client.run)
    t1.start()
    # 开启新的线程去发送操作
    t2 = threading.Thread(target=send)
    t2.start()
    # 判断操作是否超时，超时就重发操作
    while True:
        for i in range(0, len(client.operates)):
            # 状体为False（未提交的操作）
            if not client.operates[i][0]:
                # 判断操作是否超时
                if time.time() >= client.operates[i][2]:
                    # 重置操作超时时间
                    client.operates[i][2] = time.time() + client.operate_timeout
                    # 重新发送操作
                    client.send_operate(client.operates[i][1])
        time.sleep(1)

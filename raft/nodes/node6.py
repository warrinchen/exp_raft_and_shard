from raft.cluster import Cluster
from raft.start import Start
from raft.leader import Log

cluster = Cluster()
i=6
cur_term = 3
log_ar = [1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3]

if __name__ == '__main__':
    logs = []
    for j in range(0, len(log_ar)):
        logs.append(Log(log_ar[j], 0))
    Start(cluster.ids[i],
          [peer for peer in cluster if peer != id],
          cur_term, logs,
          [cluster.ids[i], (cluster.ip, cluster.ports[cluster.ids[i]])]
          ).run()

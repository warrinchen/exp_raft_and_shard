from raft.cluster import Cluster
from raft.start import Start
from raft.leader import Log

cluster = Cluster()

cur_terms = [6, 6, 4, 6, 7, 4, 3]
log_ars = [
    [1, 1, 1, ],
    [1, 1, 1, 4, 4, 5, 5, 6, 6, 6],
    [1, 1, 1, 4, 4, 5, 5, 6, 6],
    [1, 1, 1, 4],
    [1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6],
    [1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6, 7, 7 ],
    [1, 1, 1, 4, 4, 4, 4],
    [1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3],
]

if __name__ == '__main__':
    for i in range(0,7):
        logs = []
        for j in range(0, len(log_ars[i])):
            logs.append(Log(log_ars[i][j], 0))
        Start(cluster.ids[i],
              [peer for peer in cluster if peer != id],
              cur_terms[i], logs,
              [cluster.ids[i], (cluster.ip, cluster.ports[cluster.ids[i]])]
              ).run()

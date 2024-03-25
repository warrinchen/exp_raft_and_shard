import shardMaster

if __name__ == '__main__':
    # 初始化组
    groups = []
    # 初始化分片数据到组的映射
    s_g = [0 for _ in range(1, 11)]
    # 客户端获取shardMaster对象
    sm = shardMaster.SharedMaster(groups=groups, s_g=s_g)

    # Configuration:1-3
    print(f'数据分片信息：{sm.shards}')
    print(f'初始config信息：{sm.query(num=-1)}\n')
    print('加入group:1')
    sm.join(1)
    print(f'最新config信息：{sm.query(num=-1)}\n')
    print('加入group:2')
    sm.join(2)
    print(f'最新config信息：{sm.query(num=-1)}\n')
    print('加入group:5')
    sm.join(5)
    print(f'最新config信息：{sm.query(num=-1)}\n')

    # 所有config信息
    print(f'所有config信息为：')
    for i in range(0, len(sm.configs)):
        print(sm.configs[i])

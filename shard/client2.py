import shardMaster

if __name__ == '__main__':
    # 初始化组
    groups = [1, 2, 4, 5, 6, 7]
    # 初始化分片数据到组的映射
    s_g = [1, 1, 5, 2, 2, 7, 7, 4, 6, 6]
    # 客户端获取shardMaster对象
    sm = shardMaster.SharedMaster(groups=groups, s_g=s_g)

    # Configuration:10-11
    print(f'shards分片信息：{sm.shards}')
    print(f'初始config信息：{sm.query(num=-1)}\n')
    print('删除group:1')
    sm.leave(1)
    print(f'最新config信息：{sm.query(num=-1)}\n')

    # 所有config信息
    print(f'所有config信息为：')
    for i in range(0, len(sm.configs)):
        print(sm.configs[i])

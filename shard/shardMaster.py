# 生成一个config（num为config的版本，shards数据分片对应的group，groups系统中所有的组信息）
def create_config(num, shards, groups):
    if groups is None:
        # 设置系统中的所有group信息
        groups = []
    if shards is None:
        # 数据分片对应的group
        shards = []
    # 返回创建的config
    return {
        'num': num,
        'Shards': shards,
        'Groups': groups
    }


# 负载均衡方法(操作类型、所有组、分片信息、映射关系、操作的组)
def balancer(type, groups, shards, s_g, group):
    print(f'当前映射关系：{s_g}')
    if type == 'join':
        # 判断是否只有一个组
        if len(groups) == 1:
            # 将所有数据片都映射到该组
            s_g = [group for _ in range(0, len(shards))]
            print(f'加入group后的映射关系：{s_g}')
        # 判断数据片的数量是否大于组的数量
        elif len(shards) >= len(groups) > 0:
            # 获得每个组的数据片数量
            set1 = set(s_g)
            dict1 = {}
            for item in set1:
                dict1.update({item: s_g.count(item)})
            # 移动次数
            number = 1
            while True:
                # 获得数据片最多的组
                group_id = max(dict1, key=dict1.get)
                # 将该组的一个数据片移动到新加的组
                for i in range(len(s_g) - 1, -1, -1):
                    if s_g[i] == group_id:
                        s_g[i] = group
                        break
                # 获得每个组的数据片数量
                set1 = set(s_g)
                dict1 = {}
                for item in set1:
                    dict1.update({item: s_g.count(item)})
                # 判断组中最多数据片和最少数据片相差是否小于等于1
                if dict1[max(dict1, key=dict1.get)] - dict1[min(dict1, key=dict1.get)] <= 1:
                    break

                print(f'第{number}次移动后的映射关系：{s_g}')
                # 移动次数加一
                number = number + 1
            print(f'加入group后的映射关系：{s_g}')
    if type == 'leave':
        # 判断组的数量是否为0
        if len(groups) == 0:
            # 将所有数据片都取消映射
            s_g = [0 for _ in range(0, len(shards))]
            print(f'删除group后的映射关系：{s_g}')
        # 判断组的数量是否大于数据片的数量
        elif len(groups) >= len(shards) > 0:
            # 使用新的组去替换删除的组
            group_id = groups[len(shards) - 1]
            group_index = s_g.index(group)
            s_g[group_index] = group_id
            print(f'删除group后的映射关系：{s_g}')
        else:
            # 移动次数
            number = 1
            while True:
                # 获得每个组的数据片数量
                list1 = list(set(s_g))
                list1.sort(key=s_g.index)
                dict1 = {}
                for item in list1:
                    dict1.update({item: s_g.count(item)})
                # 除去待删除组的数量
                dict1.pop(group)
                # 获得数据片最少的组
                group_id = min(dict1, key=dict1.get)
                # 将待删除组的一个数据片移动到该组
                group_index = s_g.index(group)
                s_g[group_index] = group_id
                # 不存在待删除的组
                if group not in s_g:
                    break
                print(f'第{number}次移动后的映射关系：{s_g}')
                # 移动次数加一
                number = number + 1
            print(f'加入group后的映射关系：{s_g}')
    # 返回新映射关系
    return s_g


class SharedMaster:
    def __init__(self, shards=11, groups=None, s_g=None):
        # 初始化config的版本信息
        self.configs = []
        # 初始化分片数据
        self.shards = [i for i in range(1, shards)]

        # 初始化分片数据到组的映射
        if s_g is None:
            s_g = [0 for _ in range(1, shards)]
        # 初始化组
        if groups is None:
            groups = []

        # 生成初始的config
        config = create_config(0, s_g, groups)
        self.configs.append(config)

    # 加入新组的操作
    def join(self, group):
        # 获取最新配置
        config = self.configs[-1]
        groups = config['Groups'].copy()
        s_g = config['Shards'].copy()
        # 组的id是整数，并且必须是唯一的
        if isinstance(group, int) and group not in groups:
            # 添加新的group信息
            groups.append(group)
            # 对组进行数据片的负载均衡，得到新映射关系
            s_g = balancer(type='join', groups=groups, shards=self.shards, s_g=s_g, group=group)
            # 生成新的config
            config = create_config(len(self.configs), s_g, groups)
            self.configs.append(config)
            return "OK"
        else:
            return "ERROR"

    # 删除组的操作
    def leave(self, group):
        # 获取最新配置
        config = self.configs[-1]
        groups = config['Groups'].copy()
        s_g = config['Shards'].copy()
        # 判断组是否存在
        if group in groups:
            # 删除组
            groups.remove(group)
            # 判断该组是否有数据片
            if group in s_g:
                # 对组进行数据片的负载均衡，得到新映射关系
                s_g = balancer(type='leave', groups=groups, shards=self.shards, s_g=s_g, group=group)
                # 生成新的config
                config = create_config(len(self.configs), s_g, groups)
                self.configs.append(config)
                return "OK"
        else:
            return "ERROR"

    # 移动数据分片(将shard_id移动到group_id对应的组中)
    def move(self, shard_id, group_id):
        # 获取最新配置
        config = self.configs[-1]
        groups = config['Groups'].copy()
        s_g = config['Shards'].copy()
        # 判断组是否存在
        if group_id in groups:
            # 判断数据分片是否存在，并且目前所在组和要移动到的组是否相同
            if shard_id in self.shards and s_g[shard_id - 1] != group_id:
                s_g[shard_id - 1] = group_id
            return "OK"
        else:
            return "ERROR"

    # 返回configs中对应版本号的config信息
    def query(self, num):
        # 判断版本号是否为-1或大于最大的版本号
        if num == -1 or num >= len(self.configs):
            # 响应最新config
            return self.configs[-1]
        else:
            # 响应指定的config信息
            return self.configs[num]

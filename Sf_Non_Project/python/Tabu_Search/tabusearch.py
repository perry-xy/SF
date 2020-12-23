import random
from matplotlib import pyplot

class TabuSearch():
    """
    禁忌搜索求解TSP问题示例
    """
    def __init__(self, sample_length, tabu_length):
        """
        :param sample_length: 候选集合长度
        :param tabu_length: 禁忌长度
        """
        self.tabu_list = list()             # 禁忌表
        self.sample_length = sample_length  # 候选集合长度
        self.tabu_length = tabu_length      # 禁忌表长度

        self.citys, self.city_ids, self.start_id = self.loaddata()
        self.curroute = self.randomroute()  # 当前的路径，第一次初始化产生
        self.bestcost = float('inf')        # 代价函数，初始化为无穷大
        self.bestroute = None               # 代价函数最小的路径

    def loaddata(self, start_id = 1):
       """
       29个点的TSP问题
       :param start_id: 限定的出发及返回的点编号
       :return:
       """
       citys = {1: (1150.0, 1760.0), 2: (630.0, 1660.0), 3: (40.0, 2090.0), 4: (750.0, 1100.0),
                5: (750.0, 2030.0), 6: (1030.0, 2070.0), 7: (1650.0, 650.0), 8: (1490.0, 1630.0),
                9: (790.0, 2260.0), 10: (710.0, 1310.0), 11: (840.0, 550.0), 12: (1170.0, 2300.0),
                13: (970.0, 1340.0), 14: (510.0, 700.0), 15: (750.0, 900.0), 16: (1280.0, 1200.0),
                17: (230.0, 590.0), 18: (460.0, 860.0), 19: (1040.0, 950.0), 20: (590.0, 1390.0),
                21: (830.0, 1770.0), 22: (490.0, 500.0), 23: (1840.0, 1240.0), 24: (1260.0, 1500.0),
                25: (1280.0, 790.0), 26: (490.0, 2130.0), 27: (1460.0, 1420.0), 28: (1260.0, 1910.0),
                29: (360.0, 1980.0)}  # 原博客里的数据
       city_ids = list(citys.keys())

       return citys, city_ids, start_id

    def randomroute(self):
        """
        生成一条随机的路
        :return:
        """
        rt = self.city_ids.copy()
        random.shuffle(rt)
        rt.remove(self.start_id)
        rt.insert(0, self.start_id)  # 从start_id出发
        rt.append(self.start_id)  # 回到start_id

        return rt

    def randomswap(self, route):
        """
        随机交换路径的两个节点
        :param route:
        :return:
        """
        while True:
            a = random.choice(route)
            b = random.choice(route)
            if a == b or a == 1 or b == 1:   # 相同节点，或者为1号点，重新取
                continue
            ia, ib = route.index(a), route.index(b)
            route[ia], route[ib] = b, a

            return route, a, b

    def costroad(self, road):
        """
        计算当前路径的长度
        :param road:
        :return:
        """
        d = -1  # 给初始点
        st = 0, 0
        cur = 0, 0
        city = self.citys
        for v in road:
            if d == -1:
                st = city[v]  # 1号点
                cur = st
                d = 0
            else:
                d += ((cur[0] - city[v][0]) ** 2 + (
                            cur[1] - city[v][1]) ** 2) ** 0.5  # 计算所求解的距离，这里为了简单，视作二位平面上的点，使用了欧式距离
                cur = city[v]

        return d

    def step(self):
        """
        禁忌搜索
        :return:
        """
        rt = self.curroute   # 基于当前路径继续搜索

        prepare = list()     # 候选集集合
        swap_list = list()
        while len(prepare) < self.sample_length:  # 产生候选路径
            prt, swap_a, swap_b = self.randomswap(rt)
            if ([swap_a, swap_b] not in swap_list) and ([swap_b, swap_a] not in swap_list):  # 不重复子集
                swap_list.append([swap_a, swap_b])
                prepare.append(prt.copy())
        c = []
        for r in prepare:
            c.append(self.costroad(r))
        mc = min(c)
        mrt = prepare[c.index(mc)]  # 选出候选路径里最好的一条
        swap = swap_list[c.index(mc)]
        if mc < self.bestcost:   # 如果最小值小于当前最优
            self.bestcost = mc
            self.bestroute = mrt.copy()  # 如果他比最好的还要好，那么记录下来
            if (swap in self.tabu_list) or (swap[::-1] in self.tabu_list):  # 移动在禁忌表中
                if swap in self.tabu_list:
                    self.tabu_list.pop(self.tabu_list.index(swap))
                    self.tabu_list.append(swap)
                else:
                    self.tabu_list.pop(self.tabu_list.index(swap[::-1]))
                    self.tabu_list.append(swap[::-1])
            else:
                self.tabu_list.append(swap)

            self.curroute = mrt

        else:
            while (swap in self.tabu_list) or (swap[::-1] in self.tabu_list):
                swap_list.pop(swap_list.index(swap))
                prepare.pop(c.index(mc))
                c.pop(c.index(mc))
                mc = min(c)
                mrt = prepare[c.index(mc)]
                swap = swap_list[c.index(mc)]

            self.tabu_list.append(swap)
            self.curroute = mrt
            prepare = list()
            swap_list = list()

        if len(self.tabu_list) > self.tabu_length:
                self.tabu_list.pop(0)

    def plot_route(self):
        """
        画出路径图
        :return:
        """
        x = []
        y = []
        print("最优路径长度:", self.bestcost)
        for i in self.bestroute:
            x0, y0 = self.citys[i]
            x.append(x0)
            y.append(y0)
        x.append(x[0])
        y.append(y[0])
        pyplot.plot(x, y)
        pyplot.scatter(x, y)

if __name__ == '__main__':
    import timeit

    t = TabuSearch(sample_length = 200, tabu_length = 20)
    print('ok')
    print(t.citys)
    print(t.curroute)
    print(t.bestcost)
    print(t.curroute)
    for i in range(10000):
        t.step()
        if i % 500 == 0:
            print(t.bestcost)
            print(t.bestroute)
            print(t.curroute)
    t.plot_route()
    print('ok')
    # print(timeit.timeit(stmt="t.step()", number=1000,globals=globals()))
    print('ok')
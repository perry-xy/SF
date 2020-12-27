import random
from matplotlib import pyplot
from gurobipy import *
import numpy as np

class TabuSearch():
    """
    禁忌搜索求解TSP问题示例
    """
    def __init__(self, file_path, sample_length, tabu_length):
        """
        :param file_path: 数据路径
        :param sample_length: 候选集合长度
        :param tabu_length: 禁忌长度
        """
        self.file_path = file_path
        self.sample_length = sample_length  # 候选集合长度
        self.tabu_length = tabu_length      # 禁忌表长度

        self.tabu_list = list()  # 禁忌表
        self.citys, self.city_ids, self.start_id, self.distance_matrix, self.city_numbers = self.loaddata()
        self.curroute = self.randomroute()  # 当前的路径；第一次初始化产生
        self.bestcost = float('inf')        # 代价函数值；初始化为无穷大
        self.bestroute = None               # 代价函数值最小的路径

    def loaddata(self, start_id = 1):
       """
       29个点的TSP问题
       :param start_id: 限定的出发及返回的点编号
       :return:
       """
       citys = dict()
       for line in open(self.file_path):
           place, x, y = int(line.strip().split(' ')[0]), float(line.strip().split(' ')[1]), float(
               line.strip().split(' ')[2])
           citys[place] = (x, y)
       city_ids = list(citys.keys())

       distance_matrix = {}
       for src in citys.keys():
           for dest in citys.keys():
               distance = (citys[dest][0] - citys[src][0]) ** 2 + (citys[dest][1] - citys[src][1]) ** 2
               distance = np.sqrt(distance)
               distance_matrix[src, dest] = distance

       print(len(distance_matrix))
       print(distance_matrix.keys())
       return citys, city_ids, start_id, distance_matrix, len(city_ids)

    def randomroute(self):
        """
        生成一条随机的路
        :return:
        """
        rt = self.city_ids.copy()
        random.shuffle(rt)           # 打乱rt的顺序
        rt.remove(self.start_id)     # 去除起始点
        rt.insert(0, self.start_id)  # 把起始点放至第一个位置，作为出发点
        rt.remove(self.city_numbers) # 去除最后一个点
        rt.append(self.city_numbers) # 将最后一个点放至最后一个位置

        return rt

    def randomswap(self, route):
        """
        随机交换路径的两个节点
        :param route: 原始路径
        :return:
        """
        route_copy = route.copy()
        while True:
            a = random.choice(route_copy)
            b = random.choice(route_copy)
            if a == b \
                    or a == self.start_id or b == self.start_id \
                    or a == self.city_numbers or b == self.city_numbers:   # 相同节点，或者为起始点，重新取
                continue
            ia, ib = route_copy.index(a), route_copy.index(b)   # ia：a的下标; ib：b的下标
            route_copy[ia], route_copy[ib] = b, a      # a、b互换

            return route_copy, a, b               # 返回swap完后的路径，互换的两个元素

    def costroad(self, road):
        """
        计算当前路径的长度
        :param road: 路径列表
        :return:
        """
        d = -1   # 长度值
        cur = None  # 节点暂存变量
        for v in road:
            if d == -1:
                cur = self.citys[v]  # 左点第一次为1号点
                d = 0
            else:
                d += ((cur[0] - self.citys[v][0]) ** 2 + (
                            cur[1] - self.citys[v][1]) ** 2) ** 0.5  # 计算所求解的距离，这里为了简单，视作二位平面上的点，使用了欧式距离
                cur = self.citys[v]  # 更新左点

        return d

    def step(self):
        """
        禁忌搜索
        :return:
        """
        rt = self.curroute   # 基于当前路径继续搜索，第一次为随机初始化的点

        prepare = list()     # 候选集集合
        swap_list = list()   # 互换集合
        while len(prepare) < self.sample_length:  # 产生候选路径
            prt, swap_a, swap_b = self.randomswap(rt)  # 产生一次互换
            if ([swap_a, swap_b] not in swap_list) and ([swap_b, swap_a] not in swap_list) and (prt not in prepare):  # 不重复子集
                swap_list.append([swap_a, swap_b])
                prepare.append(prt)

        c = []    # 代价函数值
        for r in prepare:
            c.append(self.costroad(r))
        mc = min(c)   # 本次的最优解
        mrt = prepare[c.index(mc)]  # 最优解对应的最好路径
        swap = swap_list[c.index(mc)] # 最优解对应的互换

        if mc < self.bestcost:   # 如果最小值小于当前最优，即有了更好的解
            self.bestcost = mc
            self.bestroute = mrt.copy()  # 记录下最好结果
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

    def costroad2(self,allocation):

        locations = self.city_ids + [30]  # 1~30，加返回点一共有30个位置
        points = self.city_ids  # 1~29，29个点

        cost = sum([sum(allocation[location, point] * point for point in points) for location in locations])
        # print(solution)
        #
        # pairs = list()
        # for i in range(len(solution)-1):
        #     pairs.append((int(solution[i]), int(solution[i+1])))
        #
        # cost = 0
        # for pair in pairs:
        #     cost += self.distance_matrix[pair]

        return cost

    def exact_solution(self):
        """
        用gurobi求精确解
        :return:
        """
        locations = self.city_ids + [30]     # 1~30，加返回点一共有30个位置
        points = self.city_ids               # 1~29，29个点

        model = Model('tsp')

        allocation = model.addVars(locations, points, vtype=GRB.BINARY, name='allocation')  # 30 * 29，每个location的point

        model.addConstr(allocation[1,1] == 1, name = 'start_id')         # 规定起始点：第一个location用1号点
        model.addConstr(allocation[30,1] == 1, name = 'end_id')          # 规定终止点：最后一个location用1号点

        model.addConstrs((allocation.sum('*', point) == 1 for point in points[1:]), name='point')
                                                        # 除1号点之外，每个点一个location
        model.addConstrs((allocation.sum(location, '*') == 1 for location in locations), name='location')
                                                        # 每个location，被一个点占据
        route = [sum(allocation[location, point] * point for point in points) for location in locations]

        cost = LinExpr(0)
        for i in range(len(route) - 1):
            location = i + 1
            point_left = route[i]
            point_right = route[i+1]
            cost.addTerms(self.distance_matrix[point_left, point_right], allocation[location, point_left])

        model.setObjective(cost, sense=GRB.MINIMIZE)

        model.optimize()

        if model.Status == GRB.INFEASIBLE:

            try:
                model.computeIIS()
                model.write("model1.ilp")
            except GurobiError:
                print('infeasible model')

        else:
            print('the facility location optimized sucessfully !')
            print('the objective of the model is {}'.format(model.objVal))

        s = model.getAttr('x', allocation)
        print(s)
        print([sum(s[location, point] * point for point in points) for location in locations])

    def plot_route(self):
        """
        画出路径图
        :return:
        """
        x = []
        y = []
        print("最优路径长度:", self.bestcost)
        for i in self.bestroute:
            x0, y0 = self.citys[i][0], self.citys[i][1]
            x.append(x0)
            y.append(y0)
        pyplot.plot(x, y)
        pyplot.scatter(x, y)
        pyplot.savefig('tbsearch_image')

if __name__ == '__main__':
    import timeit

    t = TabuSearch(file_path = 'data.txt', sample_length = 200, tabu_length = 19)
    print('ok')
    print(t.citys)
    print(t.curroute)
    print(t.bestcost)
    print(t.curroute)
    for i in range(10001):
        t.step()
        if i % 500 == 0:
            print(t.bestcost)
            print(t.bestroute)
            print(t.curroute)
    t.plot_route()
    print('ok')
    print(timeit.timeit(stmt="t.step()", number=1000,globals=globals()))
    print('ok')
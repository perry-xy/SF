# @Time : 2020/12/26 10:25 下午 
# @Author : Xingyou Chen
# @File : tsp_exact.py 
# @Software: PyCharm
from gurobipy import *
import numpy as np
from matplotlib import pyplot

class TspExact():
    """
    Tsp问题的精确解求法
    """
    def __init__(self, filepath):
        """
        :param filepath:数据路径
        """
        self.filepath = filepath

        self.citys, self.city_ids, self.distance_matrix = self.loaddata()
        self.city_numbers = len(self.city_ids)

    def loaddata(self):
        """
        各地方坐标点生成距离矩阵
        """
        citys = {}
        for line in open(self.filepath):
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

        return citys, city_ids, distance_matrix

    def model(self):
        """
        建模并用gurobi求解
        :return:
        """
        model = Model('TSP')  # 定义模型
        ## 添加决策变量
        x = model.addVars(self.city_ids, self.city_ids, vtype = GRB.BINARY, name = 'route')
        mu = model.addVars(self.city_ids, vtype = GRB.CONTINUOUS, name = 'mu')

        ## 添加约束条件
        # 1.每个点都需要出发一次（除了最后一个点）
        for i in set(self.city_ids) - {max(self.city_ids)}:  # 剔除最后一个点
            sum = LinExpr(0)
            for j in self.city_ids:
                if i != j:
                    sum.addTerms(1, x[i, j])
            model.addConstr(sum == 1, name = 'all_start_{}'.format(i))
        # 2.每个点都需要到达一次（除了第一个点）
        for i in set(self.city_ids) - {min(self.city_ids)}:  # 剔除第一个点
            sum = LinExpr(0)
            for j in self.city_ids:
                if i != j:
                    sum.addTerms(1, x[j, i])
            model.addConstr(sum == 1, name = 'all_end_{}'.format(i))
        # 3. 关于mu的约束
        for i in set(self.city_ids) - {max(self.city_ids)}:
            for j in set(self.city_ids) - {min(self.city_ids)}:
                if i != j:
                    model.addConstr(mu[i] - mu[j] + self.city_numbers * x[i, j] <= self.city_numbers - 1)

        ## 添加目标函数
        cost = LinExpr(0)
        for i in self.city_ids:
            for j in self.city_ids:
                cost.addTerms(self.distance_matrix[i, j], x[i, j])

        model.setObjective(cost, GRB.MINIMIZE)

        model.write('model.lp')
        model.optimize()

        x_i_j = model.getAttr('x', x)
        choose_xij = {key: value for key, value in x_i_j.items() if value > 0.5}

        return choose_xij

    def getroute(self, choose_xij):
        """
        提取出路径
        :param choose_xij: 被选中的出发_到达路径
        :return:
        """
        cost = 0
        for i in choose_xij:
            cost += self.distance_matrix[i]
        print('最短路径为：{}'.format(cost))

        # 判断第一个点是出发还是到达
        if (min(self.city_ids), max(self.city_ids)) in choose_xij.keys():  # 第一个点为到达点
            point = max(self.city_ids)
        else:   point = min(self.city_ids)        # 最后一个点为到达点

        route = [point]         # 起始点
        while len(route) < self.city_numbers:
            for xij in choose_xij.keys():
                if xij[0] == point:
                    route.append(xij[1])
                    point = xij[1]

        print('最优路径为：{}'.format(route))
        print(len(route))

        return route

    def plotroute(self, route):
        """
        画出路径图
        :param route: 路径
        :return:
        """
        x = []
        y = []
        for i in route:
            x0, y0 = self.citys[i][0], self.citys[i][1]
            x.append(x0)
            y.append(y0)
        pyplot.plot(x, y)
        pyplot.scatter(x, y)
        pyplot.savefig('exactsolution_image')

if __name__ == '__main__':
    t = TspExact('data.txt')
    solution = t.model()
    route = t.getroute(solution)
    t.plotroute(route)





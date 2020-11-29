from gurobipy import *
import xlrd
import json
import math
from pyecharts.charts import Geo
from pyecharts import options as opts
from pyecharts.globals import GeoType


def calculate_distance(lng1, lat1, lng2, lat2):  # 经度和纬度 longitude and latitude，计算球上两点的距离
    RADIUS = 6378.137  # 半径，单位km
    PI = math.pi
    return 2 * RADIUS * math.asin(math.sqrt(pow(math.sin(PI * (lat1 - lat2) / 360), 2) + math.cos(PI * lat1 / 180) * \
                                            math.cos(PI * lat2 / 180) * pow(math.sin(PI * (lng1 - lng2) / 360), 2)))


def get_distance():
    with open(u'D:/code/Python/猫人仓网规划/location1.json', 'r') as f_obj:
        geo_cities_coords = json.load(f_obj)

    data = xlrd.open_workbook(u'D:/code/Python/猫人仓网规划/t.xlsx')
    table2 = data.sheets()[0]
    nrows = table2.nrows  # 读取第一个工作表的行数
    city1 = []
    demand = []
    for i in range(2, nrows):
        row = table2.row_values(i)
        city1.append(row[0][:-1])
        demand.append(row[2])
    print(city1)

    city2 = list(geo_cities_coords.keys())
    distance = [[float('inf') for _ in range(len(geo_cities_coords))] for _ in range(len(city1))]
    for row, c1 in enumerate(city1):
        for col, c2 in enumerate(city2):
            dis = calculate_distance(geo_cities_coords[c1][0], geo_cities_coords[c1][1], geo_cities_coords[c2][0],
                                     geo_cities_coords[c2][1])
            distance[row][col] = dis

    return distance, demand, city1, city2, geo_cities_coords


def build_model(distance, demand, demand_city, total_city, geo_cities_coords):
    # 每单位每公里固定费用
    k_fee = 0.01
    # 建仓固定费用
    w_fee = 100000

    demand_len, citys_len = len(distance), len(distance[0])

    model = Model()
    x = model.addVars(range(demand_len), range(citys_len), vtype=GRB.BINARY)
    w = model.addVars(range(citys_len), vtype=GRB.BINARY)

    # 创建目标函数
    model.setObjective((quicksum(w_fee * w[j] for j in range(citys_len)) +
                        quicksum(k_fee * distance[i][j] * demand[i] * x[i, j] for i in range(demand_len)
                                 for j in range(citys_len))), sense=GRB.MINIMIZE)

    # (1)保证每个需求点都包含
    for i in range(demand_len):
        model.addConstr(quicksum(x[i, j] for j in range(citys_len)) == 1)

    # (2)需求点不去为建仓城市取货
    for i in range(demand_len):
        for j in range(citys_len):
            model.addConstr(x[i, j] <= w[j])

    # 设定模型求解时间100s
    model.params.TimeLimit = 100
    model.optimize()

    for i in range(citys_len):
        if w[i].x > 0.5:
            print(total_city[i])

    plotting_data = []
    location = {}
    for i in range(citys_len):
        if w[i].x > 0.5:
            for j in range(demand_len):
                if x[j, i].x > 0.5:
                    if total_city[i] != demand_city[j]:
                        plotting_data.append((total_city[i], demand_city[j]))

                        location[total_city[i]] = geo_cities_coords[total_city[i]]
                        location[demand_city[j]] = geo_cities_coords[demand_city[j]]
    plot_geolines(plotting_data, location)


def plot_geolines(plotting_data, geo_cities_coords):
    c = Geo(init_opts=opts.InitOpts(width='1500px', height='700px'))
    c.add_schema(
        maptype="china",
        itemstyle_opts=opts.ItemStyleOpts(color="#F0CA00", border_color="#111"))
    # 所有坐标点加进地图
    for i in range(0, len(geo_cities_coords)):
        c.add_coordinate(list(geo_cities_coords.keys())[i], list(geo_cities_coords.values())[i][0],
                         list(geo_cities_coords.values())[i][1])
    c.add('仓网结构', plotting_data,
          type_=GeoType.LINES,
          linestyle_opts=opts.LineStyleOpts(width=1),
          label_opts=opts.LabelOpts(is_show=False),
          ##trail_length=0,
          is_polyline=True)
    c.set_global_opts(title_opts=opts.TitleOpts(title="仓网结构"))
    c.render('二级仓网结构.html')


if __name__ == "__main__":
    try:
        distance, demand, demand_city, total_city, geo_cities_coords = get_distance()

        build_model(distance, demand, demand_city, total_city, geo_cities_coords)

    except GurobiError as exception:
        print('Error code ' + str(exception.errno) + ": " + str(exception))

    except AttributeError:
        print('Encountered an attribute error')
# -*- coding: utf-8 -*-

from gurobipy import *

import xlrd
import json
import math
from pyecharts import GeoLines, Style


def calculate_distance(lng1, lat1, lng2, lat2):  # 经度和纬度 longitude and latitude，计算球上两点的距离
    RADIUS = 6378.137  # 半径，单位km
    PI = math.pi
    return 2 * RADIUS * math.asin(math.sqrt(pow(math.sin(PI * (lat1 - lat2) / 360), 2) + math.cos(PI * lat1 / 180) * \
                                            math.cos(PI * lat2 / 180) * pow(math.sin(PI * (lng1 - lng2) / 360), 2)))


def get_distance():
    with open('location1.json', 'r') as f_obj:
        geo_cities_coords = json.load(f_obj)

    data = xlrd.open_workbook('t.xlsx')
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


def plot_geolines(plotting_data, geo_cities_coords):
    # 设置画布的格式
    style = Style(title_pos="center",
                  width=1000,
                  height=800)

    # 部分地理轨迹图的格式
    style_geolines = style.add(is_label_show=True,
                               line_curve=0.3,  # 轨迹线的弯曲度，0-1
                               line_opacity=0.6,  # 轨迹线的透明度，0-1
                               geo_effect_symbol='plane',  # 特效的图形，有circle,plane,pin等等
                               geo_effect_symbolsize=5,  # 特效图形的大小
                               geo_effect_color='#7FFFD4',  # 特效的颜色
                               geo_effect_traillength=0.1,  # 特效图形的拖尾效果，0-1
                               label_color=['#FFA500', '#FFF68F'],  # 轨迹线的颜色，标签点的颜色，
                               border_color='#97FFFF',  # 边界的颜色
                               geo_normal_color='#36648B',  # 地图的颜色
                               label_formatter='{b}',  # 标签格式
                               legend_pos='left')

    # 作图
    geolines = GeoLines('仓网规划图', **style.init_style)
    geolines.add('',
                 plotting_data,
                 maptype='china',  # 地图的类型，可以是省的地方，如'广东',也可以是地市，如'东莞'等等
                 geo_cities_coords=geo_cities_coords,
                 **style_geolines)

    # 发布，得到图形的html文件
    geolines.render()


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
                        quicksum(k_fee * distance [i][j] * demand[i] * x[i, j] for i in range(demand_len)
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
                if x[j, i].x>0.5:
                    if total_city[i] != demand_city[j]:
                        plotting_data.append((total_city[i], demand_city[j]))

                        location[total_city[i]] = geo_cities_coords[total_city[i]]
                        location[demand_city[j]] = geo_cities_coords[demand_city[j]]
    plot_geolines(plotting_data, location)


if __name__ == "__main__":
    try:
        distance, demand, demand_city, total_city, geo_cities_coords = get_distance()

        build_model(distance, demand, demand_city, total_city, geo_cities_coords)

    except GurobiError as exception:
        print('Error code ' + str(exception.errno) + ": " + str(exception))

    except AttributeError:
        print('Encountered an attribute error')
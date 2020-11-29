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

    city2_ = list(geo_cities_coords.keys())
    city2 = city2_[60:]
    ij_distance = [[float('inf') for _ in range(len(city2))] for _ in range(len(city1))]
    for row, c1 in enumerate(city1):
        for col, c2 in enumerate(city2):
            dis = calculate_distance(geo_cities_coords[c1][0], geo_cities_coords[c1][1], geo_cities_coords[c2][0],
                                    geo_cities_coords[c2][1])
            ij_distance[row][col] = dis

    data = xlrd.open_workbook('gc.xlsx')
    table2 = data.sheets()[0]
    nrows = table2.nrows  # 读取第一个工作表的行数
    city3 = []
    city3 = city2_[:55]
    # for i in range(1, nrows):
    #     row = table2.row_values(i)
    #     city3.append(row[1])
    # print(city3)

    jk_distance = [[float('inf') for _ in range(len(city3))] for _ in range(len(city2))]
    for row, c2 in enumerate(city2):
        for col, c3 in enumerate(city3):
            dis = calculate_distance(geo_cities_coords[c2][0], geo_cities_coords[c2][1], geo_cities_coords[c3][0],
                                     geo_cities_coords[c3][1])
            jk_distance[row][col] = dis

    return ij_distance, jk_distance, demand, city1, city2, city3, geo_cities_coords


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
    geolines.render("仓网规划图2.html")


def build_model(ij_distance, jk_distance, demand, city1, city2, city3, geo_cities_coords):
    # 每单位每公里固定费用
    k_fee = 0.01
    k_fee1 = 0.006
    # 建仓固定费用
    w_fee = 100000
    z_fee = 100000000

    demand_len, dc_citys_len = len(ij_distance), len(ij_distance[0])
    gc_citys_len = len(jk_distance[0])

    model = Model()
    x = model.addVars(range(demand_len), range(dc_citys_len), vtype=GRB.CONTINUOUS)
    y = model.addVars(range(dc_citys_len), range(gc_citys_len), vtype=GRB.CONTINUOUS)
    w = model.addVars(range(dc_citys_len), vtype=GRB.BINARY)
    z = model.addVars(range(gc_citys_len), vtype=GRB.BINARY)

    # 创建目标函数
    model.setObjective((quicksum(w_fee * w[j] for j in range(dc_citys_len)) +
                        quicksum(z_fee * z[k] for k in range(gc_citys_len)) +
                        quicksum(k_fee * ij_distance[i][j] * x[i, j] for i in range(demand_len)
                                 for j in range(dc_citys_len)) +
                        quicksum(k_fee1 * jk_distance[j][k] * y[j, k] for j in range(dc_citys_len)
                                 for k in range(gc_citys_len))
                        ), sense=GRB.MINIMIZE)

    # (1)保证每个需求点都包含
    for i in range(demand_len):
        model.addConstr(quicksum(x[i, j] for j in range(dc_citys_len)) == demand[i])

    # (2)需求点不去为建仓城市取货,同时满足货量小于仓库容量
    for j in range(dc_citys_len):
        model.addConstr(quicksum(x[i, j] for i in range(demand_len)) <= 18000*w[j])

    # (3)每个工厂和仓库有对应关系
    for j in range(dc_citys_len):
        model.addConstr(quicksum(y[j, k] for k in range(gc_citys_len)) == quicksum(x[i, j] for i in range(demand_len)))

    # (4)仓库不去未建工厂的城市取货，同时满足货量小于工厂容量
    for k in range(gc_citys_len):
        model.addConstr(quicksum(y[j, k] for j in range(dc_citys_len)) <= 18000*z[k])

    # 设定模型求解时间600s
    model.params.TimeLimit = 200
    model.optimize()

    print('DC:')
    for i in range(dc_citys_len):
        if w[i].x > 0.5:
            print(city2[i])

    print('GC:')
    for i in range(gc_citys_len):
        if z[i].x > 0.5:
            print(city3[i])

    plotting_data = []
    location = {}
    for j in range(gc_citys_len):
        if z[j].x > 0.5:
            for k in range(dc_citys_len):
                if y[k, j].x>0.5:
                    if city3[j] != city2[k]:
                        plotting_data.append((city3[j], city2[k]))

                        location[city3[j]] = geo_cities_coords[city3[j]]
                        location[city2[k]] = geo_cities_coords[city2[k]]

    for i in range(dc_citys_len):
        if w[i].x > 0.5:
            for j in range(demand_len):
                if x[j, i].x>0.5:
                    if city2[i] != city1[j]:
                        plotting_data.append((city2[i], city1[j]))

                        location[city2[i]] = geo_cities_coords[city2[i]]
                        location[city1[j]] = geo_cities_coords[city1[j]]
    plot_geolines(plotting_data, location)

# 15. 检索" 01 "课程分数小于 60，按分数降序排列的学生信息
f = """
select
    t1.s_id,
    t1.s_name,
    t3.avg_score
from
    student t1,
    (
        select t2.s_id,
        round(avg(s_score)) as avg_score
    from
        (
        select
            *
        from
            score
        where
            s_score < '60') t2
    group by
        t2.s_id
    having
        count(t2.s_id) >= 2) t3
where
    t1.s_id = t3.s_id
"""

if __name__ == "__main__":
    ij_distance, jk_distance, demand, city1, city2, city3, geo_cities_coords = get_distance()

    build_model(ij_distance, jk_distance, demand, city1, city2, city3, geo_cities_coords)

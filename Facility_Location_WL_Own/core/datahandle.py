# -*- coding: utf-8 -*-
"""
Created on Fri Oct 16 21:05:42 2020

@author: 01397192
"""

import pandas as pd
import gc
from pyecharts import options as opts
from pyecharts.charts import Map
import datetime
import xlwings as xw

class DataHandler():
    """
        数据预处理
                    """

    def __init__(self, filename):
        """
            filename 文件名
                                """
        self.filename = filename
        self.demand = dict()  # 需求点数据
        self.sku = dict()  # sku信息
        self.warehouse = dict()  # 候选仓信息
        self.distribution_price = dict()  # 支线运输价格
        self.trunk_price = dict()  # 干线运输价格
        self.cdc_category_capacity = dict()  # 供应商产能
        self.GIS = dict()  # 地图
        self.customer = list()  # 所有需求点
        self.category_list = list()  # sku类型
        self.rdc_cand = list()  # 候选RDC
        self.cdc_cand = list()  # 候选供应商
        self.warehouse_area_ratio = dict()  # 仓变系数
        self.reverse_tob = dict() #调拨的部分
        self.reverse_price = dict() #调拨部分的to_B价格
        self.reverse_customer = list() #接受调拨的区仓
        self.dis_price_toB_transit = dict() #支线配送中转价格
        self.dis_price_toB_dist = dict()  # 支线配送支线价格
        self.dis_price_toB_trunk = dict() # 支线配送干线价格

        self.handle()

    def handle(self):
        """
            类型转换；编写字典
                                """
        print("开始读取数据："+str(datetime.datetime.now()))

        app = xw.App(visible=False, add_book=False)
        wb = app.books.open(self.filename)

        demand_df = wb.sheets["Demand_toC"].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                               dtype=object).value
        sku_df =wb.sheets['SKU_Info'].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                               dtype=object).value
        distribution_price_df = wb.sheets['Distribution'].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                               dtype=object).value
        warehouse_df = wb.sheets['Warehouse'].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                               dtype=object).value
        trunk_price_df = wb.sheets['Trunk'].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                               dtype=object).value
        factory_capacity_df = wb.sheets['Factory'].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                               dtype=object).value
        gis = wb.sheets['GIS'].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                               dtype=object).value
        warehouse_area_ratio = wb.sheets['仓变系数'].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                               dtype=object).value
        reverse_tob_df = wb.sheets['Reverse_toB'].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                               dtype=object).value
        # reverse_price_df = wb.sheets['Reverse_Price'].range('A1').options(pd.DataFrame, expand='table', index=False,
        #                                                        dtype=object).value
        reverse_price_df = pd.read_csv("data/Reverse_Price.csv")
        dis_price_toB_transit_df = wb.sheets['Dis_Transit'].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                               dtype=object).value
        dis_price_toB_dist_df = wb.sheets['Dis_Dist'].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                               dtype=object).value
        dis_price_toB_trunk_df = wb.sheets['Dist_Trunk'].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                               dtype=object).value

        print("数据读取结束：" + str(datetime.datetime.now()))

        demand_df[['城市代码']] = demand_df[['城市代码']].astype(int).astype(str)
        distribution_price_df[['src_code', 'dest_code']] = distribution_price_df[['src_code', 'dest_code']].astype(
            int).astype(str)
        warehouse_df['城市代码'] = warehouse_df['城市代码'].astype(int).astype(str)
        trunk_price_df[['src_code', 'dest_code']] = trunk_price_df[['src_code', 'dest_code']].astype(int).astype(str)
        factory_capacity_df[['城市代码']] = factory_capacity_df[['城市代码']].astype(int).astype(str)
        gis['城市代码'] = gis['城市代码'].astype(int).astype(str)
        warehouse_area_ratio['仓数'] = warehouse_area_ratio['仓数'].astype(int)
        reverse_tob_df[['城市代码','车型']] = reverse_tob_df[['城市代码','车型']].astype(int).astype(str)
        reverse_price_df[['src_code', 'dest_code', '车型']] = reverse_price_df[['src_code', 'dest_code', '车型']].astype(int).astype(str)
        dis_price_toB_transit_df[['src_code', 'dest_code']] = dis_price_toB_transit_df[['src_code', 'dest_code']].astype(int).astype(str)
        dis_price_toB_dist_df['始发城市'] = dis_price_toB_dist_df['始发城市'].astype(int).astype(str)
        dis_price_toB_trunk_df[['src_code', 'dest_code']] = dis_price_toB_trunk_df[['src_code', 'dest_code']].astype(int).astype(str)

        wb.close()
        app.quit()

        # sku、category_list
        for idx, row in sku_df.iterrows():
            sku_code = row['SKU代码'].lower()
            self.category_list.append(sku_code)
            self.sku[sku_code] = {'name': row['SKU名称'],
                                  'turnoverdays': row['周转天数'],
                                  'area_weight_ratio': row['面积重量比（m^2/kg)']}
        self.category_list.sort()

        # demand、customer
        for idx, row in demand_df.iterrows():
            self.customer.append(row['城市代码'])
            self.demand[row['城市代码']] = dict()
            for sku in self.category_list:
                self.demand[row['城市代码']][sku] = row[sku.upper()]
            self.demand[row['城市代码']]['weight'] = row['总重量']
        print(len(self.customer))

        # warehouse、rdc_cand
        for idx, row in warehouse_df.iterrows():
            self.rdc_cand.append(row['城市代码'])
            self.warehouse[row['城市代码']] = {'rental': row['租金（元/月/平方米)'],
                                           'upper_area': row['面积上限'],
                                           'ware_in_fee': row['入库操作费用（元/公斤）'],
                                           'ware_out_fee': row['出库操作费用（元/公斤）']}

        # distribution_price
        for idx, row in distribution_price_df.iterrows():
            src = row['src_code']
            dest = row['dest_code']
            self.distribution_price[(src, dest)] = {'distance': row['distance'],
                                                    'base_qty': row['首重'],
                                                    'base_price': row['首重价格'],
                                                    'weight_price_qty': row['续重价格']}

        # trunk_price
        for idx, row in trunk_price_df.iterrows():
            src = row['src_code']
            dest = row['dest_code']
            self.trunk_price[(src, dest)] = {'trans_fee': row['price'],
                                             'distance': row['distance']}

        # factory_capacity、cdc_cand
        for idx, row in factory_capacity_df.iterrows():
            self.cdc_cand.append(row['城市代码'])
            self.cdc_category_capacity[row['城市代码']] =dict()
            for sku in self.category_list:
                self.cdc_category_capacity[row['城市代码']][sku] = row[sku.upper()]

        # GIS
        for idx, row in gis.iterrows():
            self.GIS[row['城市代码']] = {'city_name_cn': row['城市名'],
                                     'city_name_en': row['city_name'],
                                     'lng': row['lng'],
                                     'lat': row['lat'],
                                     'province': row['省']}

        # warehouse_area_ratio
        for idx, row in warehouse_area_ratio.iterrows():
            self.warehouse_area_ratio[row['仓数']] = row['系数']

        # reverse_tob
        for idx,row in reverse_tob_df.iterrows():
            self.reverse_customer.append(row['城市代码'])
            self.reverse_tob[row['城市代码']] =dict()
            for sku in self.category_list:
                self.reverse_tob[row['城市代码']][sku] = row[sku.upper()]
            self.reverse_tob[row['城市代码']]['weight'] = row['需求重量']
            self.reverse_tob[row['城市代码']]['vehicle'] = row['车型']

        # reverse_price
        for idx, row in reverse_price_df.iterrows():
            src = row['src_code']
            dest = row['dest_code']
            vehicle = row['车型']
            self.reverse_price[(src,dest,vehicle)] = {'distance':row['distance'],
                                                      'trans_fee':row['price/kg']}

        # dis_price_toB_transit
        for idx, row in dis_price_toB_transit_df.iterrows():
            src = row['src_code']
            dest = row['dest_code']
            self.dis_price_toB_transit[(src,dest)] = {'transit_fee':row['price']}

        # dis_price_toB_dist
        for idx, row in dis_price_toB_dist_df.iterrows():
            self.dis_price_toB_dist[row['始发城市']] = {'dist_fee':row['元/kg']}

        # dis_price_toB_trunk
        for idx, row in dis_price_toB_trunk_df.iterrows():
            src = row['src_code']
            dest = row['dest_code']
            self.dis_price_toB_trunk[(src,dest)] = {'trans_fee':row['price'],
                                                      'distance':row['distance']}

        del demand_df
        del sku_df
        del warehouse_df
        del distribution_price_df
        del trunk_price_df
        del factory_capacity_df
        del gis
        del warehouse_area_ratio
        del reverse_tob_df
        del reverse_price_df
        del dis_price_toB_transit_df
        del dis_price_toB_dist_df
        del dis_price_toB_trunk_df
        gc.collect()

        print("数据处理完毕：" + str(datetime.datetime.now()))



class DemandVisualization(DataHandler):
    """
    需求分布热力图
    """

    def __init__(self, filename):
        super().__init__(filename)

    def demandvisual(self):

        """
            省份需求热力图
                        """
        demand_df = pd.DataFrame(self.demand).T
        GIS_df = pd.DataFrame(self.GIS).T

        demand_gis_df = pd.merge(demand_df, GIS_df, "inner", left_on=demand_df.index, right_on=GIS_df.index)
        province_demand_df = demand_gis_df.groupby(['province'])['weight'].agg([('weight', 'sum')])

        province = list(province_demand_df.index)
        weight = list(province_demand_df['weight'])

        # 图例分段
        # piece = [{'max': 500000},
        #          {'min': 600000, 'max': 1500000},
        #          {'min': 1600000, 'max': 2600000},
        #          {'min': 3000000, 'max': 6000000},
        #          {'min': 8000000, 'max': 16000000},
        #          {'min': 30000000}]

        # 定义Map()
        c = (
            Map(init_opts=opts.InitOpts(width="1200px", height="800px"))  ##初始化配置项
                .add("province_demand_heatmap", [list(z) for z in zip(province, weight)], maptype="china")
                .set_global_opts(
                title_opts=opts.TitleOpts(title="Demand_Heatmap"),
                visualmap_opts=opts.VisualMapOpts(is_show=True,min_=0,max_=max(weight),pos_top='middle'))
            # visualmap_opts=opts.VisualMapOpts(is_piecewise=True, pieces=piece)
            .render("物料专配需求分布.html")
        )


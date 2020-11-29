#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
# @Time    : 2019/9/9 9:54
# @Author  : peng.wang
# @Email   : WangPeng4@sfmail.sf-express.com
# @FileName: util.py
# @ProjectName :Facility_Location_FangTai
"""

import pandas as pd
import os
import xlwings as xw
from utils.misc import Logger
from pyecharts import options as opts
from pyecharts.charts import Geo
from pyecharts.globals import ChartType, SymbolType
# define the log file
RAW_DATA_PATH = os.path.dirname(os.path.dirname(__file__))
log = Logger(log_path= os.path.join(RAW_DATA_PATH, 'log')).logger


class DataHandler(object):
    """

    """

    def __init__(self, file, config):
        """
        file:文件名
        :param file:
        :param config
        """
        # 读数路径
        self._PATH = os.path.join(os.path.join(RAW_DATA_PATH, 'data'), file)
        self._config = config
        self._load_rawdata()
        self._process_demand()
        self._process_dc()
        self._process_sku()
        self._process_factory()
        self._process_shipment()
        # self._process_reverse()
        # self._process_warehose_coeff()
        self._process_gis()
        self._process_warehose_coeff()

    def _load_rawdata(self):
        """
        load the raw data from excel file
        :return:
        """
        # another method for load excel data
        log.info('loading the excel file ......')
        app = xw.App(visible=False, add_book=False)
        wb = app.books.open(self._PATH)
        try:
            # '需求表toC'
            self._end_df_2c = wb.sheets["Demand_toC"].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                               dtype=object).value
            self._end_df_2c['城市代码'] = self._end_df_2c['城市代码'].astype(int).astype(str)
            self._end_df_2c.fillna(0, inplace=True)
            # _sku_info
            self._sku_info = wb.sheets["SKU_Info"].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                   dtype=object).value
            # self._sku_info['城市代码'] = self._end_df_2b['城市代码'].astype(int).astype(str)
            self._sku_info.fillna(0, inplace=True)
            # 候选仓信息
            # 仓库表
            self._dc_df = wb.sheets["Warehouse"].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                   dtype=object).value
            self._dc_df.fillna(0, inplace=True)
            self._dc_df['城市代码'] = self._dc_df['城市代码'].astype(int).astype(str)
            # 供应商列表
            self._factory_df = wb.sheets['Factory'].range("A1").options(pd.DataFrame, expand='table', index=False,
                                                   dtype=object).value
            self._factory_df['城市代码'] = self._factory_df['城市代码'].astype(int).astype(str)
            # 干线运输
            self._trunk_infor = wb.sheets["Trunk"].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                   dtype=object).value
            self._trunk_infor.fillna(0, inplace=True)
            self._trunk_infor['始发城市'] = self._trunk_infor['始发城市'].astype(int).astype(str)
            self._trunk_infor['目的地城市'] = self._trunk_infor['目的地城市'].astype(int).astype(str)
            # 支线配送
            self._transfer_infor = wb.sheets["Distribution"].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                   dtype=object).value
            self._transfer_infor.fillna(0, inplace=True)
            self._transfer_infor['始发城市'] = self._transfer_infor['始发城市'].astype(int).astype(str)
            self._transfer_infor['目的地城市'] = self._transfer_infor['目的地城市'].astype(int).astype(str)
            # 城市配置
            self._city_gis = wb.sheets["GIS"].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                               dtype=object).value
            self._city_gis['城市代码'] = self._city_gis['城市代码'].astype(int).astype(str)
            self._city_gis.fillna(0, inplace=True)
            # 仓便系数
            self._warehouse_coeff = wb.sheets["Num_Wareshouse"].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                   dtype=object).value
            self._warehouse_coeff.fillna(0, inplace=True)

            wb.close()
            app.quit()
            log.info('load the excel file over')
        except Exception as e:
            log.error('load the excel file error {}'.format(e))
            wb.close()
            app.quit()

    def _process_factory(self):
        """
        处理工厂的情况
        :return:
        """
        self.cdc_use, self.cdc_current = [], []
        self.cdc_cand = []
        self.cdc_category_capacity = {}

        for idx, row in self._factory_df.iterrows():
            name = row['城市代码']
            self.cdc_cand.append(name)
            if row['是否必须使用'] == 1:
                self.cdc_use.append(name)
            # elif row['仓库类型']==0:
            #     self.cdc_cand.append(name)
            #     if row['是否必须使用'] == 1:
            #         self.cdc_use.append(name)
            #     if row['是否现有仓'] == 1:
            #         self.cdc_current.append(name)
            # else:
            #     self.rdc_cand.append(name)
            #     if row['是否必须使用'] == 1:
            #         self.rdc_use.append(name)
            #     if row['是否现有仓'] == 1:
            #         self.rdc_current.append(name)

            if name not in self.cdc_category_capacity:
                self.cdc_category_capacity[name] = {
                    # 'capacity': row['产能'],
                    'SKU1': row['SKU1'],
                    'SKU2': row['SKU2'],
                    'SKU3': row['SKU3'],
                    'SKU4': row['SKU4'],
                    'SKU5': row['SKU5'],
                    # 'SKU6': row['SKU6'],
                }

            self.cdc_current = list(set(self.cdc_current))
            self.cdc_use = list(set(self.cdc_use))
            self.cdc_cand = list(set(self.cdc_cand))

    def _process_dc(self):
        """
        仓库处理这里分成使用中, 必选点, 候选点
        :return:
        """
        # Define return
        self.rdc_use, self.rdc_current = [], []
        # self.cdc_use, self.cdc_current = [], []
        self.rdc_cand = []
        # self.cdc_cand = []
        self.rdc_cost_loc = {}
        self.rdc_capacity = {}
        # Process raw data
        # self._dc_df['CITY_CODE'] = self._dc_df['CITY_CODE'].apply(
        #     lambda x: '0' + x if x.__len__() == 2 else x)
        # Compose return
        for idx, row in self._dc_df.iterrows():
            name = row['城市代码']
            if row['仓库类型']==1:
                self.rdc_cand.append(name)
                if row['是否必须使用'] == 1:
                    self.rdc_use.append(name)
                if row['是否现有仓'] == 1:
                    self.rdc_current.append(name)
            # elif row['仓库类型']==0:
            #     self.cdc_cand.append(name)
            #     if row['是否必须使用'] == 1:
            #         self.cdc_use.append(name)
            #     if row['是否现有仓'] == 1:
            #         self.cdc_current.append(name)
            # else:
            #     self.rdc_cand.append(name)
            #     if row['是否必须使用'] == 1:
            #         self.rdc_use.append(name)
            #     if row['是否现有仓'] == 1:
            #         self.rdc_current.append(name)

            if name not in self.rdc_cost_loc:
                self.rdc_cost_loc[name] = {
                    # 'unit_area': row['件面比（件/平米）'],
                    'monthly_rental_price': row['租金成本/月/平米'],
                    'in_handling_cost': row['入仓(元/千克）'],
                    'out_handling_cost': row['出仓(元/千克）'],
                    # 'category': row['产品类型'],
                    # 'capacity': row["仓最大库存量"],
                    # 'replenishment': row['补货次数'],
                    # "unit_area_toC": row['toC_件面比（件/平米）'],
                    # "unit_area_toB": row['toB_件面比（件/平米）'],
                    # 'inbound_carton_price_toB': row[' toB_入仓(元/件）'],
                    # 'outbound_carton_price_toB': row['toB_出仓/（元/件）'],
                    # 'inbound_carton_price_toC': row['toC_入仓(元/件）'],
                    # 'outbound_order_price_toC': row['toC_出仓/（元/件）'],
                    # "inbound_price_reverse_toC": row['toC_逆向_入仓(元/件）'],
                    # "inbound_price_reverse_toB": row[' toB_逆向_入仓(元/件）'],
                }
            # else:
                # self.rdc_cost_loc[name]['monthly_rental_price'] = round(
                #     (row['租金成本/月/平米'] + self.rdc_cost_loc[name]['monthly_rental_price']) / 2, 2)
                # self.rdc_cost_loc[name]['inbound_piece_price'] = round(
                #     (row['入仓(元/件）'] + self.rdc_cost_loc[name]['inbound_piece_price']) / 2, 2)
                # self.rdc_cost_loc[name]['outbound_order_price'] = round(
                #     (row['出仓/（元/件）'] + self.rdc_cost_loc[name]['outbound_order_price']) / 2,
                #     2)

            if name not in self.rdc_capacity:
                if row['仓最大库存量'] == 0:
                    self.rdc_capacity[name] = self._config.rdc_capacity
                else:
                    self.rdc_capacity[name] = row['仓最大库存量']
            else:
                if row['仓最大库存量'] == 0:
                    self.rdc_capacity[name] += self._config.rdc_capacity
                else:
                    self.rdc_capacity[name] += row['仓最大库存量']

        self.rdc_current = list(set(self.rdc_current))
        self.rdc_use = list(set(self.rdc_use))
        self.rdc_cand = list(set(self.rdc_cand))

        # self.cdc_current = list(set(self.cdc_current))
        # self.cdc_use = list(set(self.cdc_use))
        # self.cdc_cand = list(set(self.cdc_cand))
    def _process_sku(self):
        """
        进行 sku 信息的处理
        :return:
        """
        self.category_info = {}
        for idx, row in self._sku_info.iterrows():
            sku = row["category"]
            self.category_info[sku] = {"weight": row['weight'],
                                       "turn_over_day": row["turn_over_day"],
                                       "safety_inventory": row['safety_inventory'],
                                       "area": row['area']
                                       }

    def _process_shipment(self):
        """
        process the original trunk line and distribution price
        :return:
        """
        self.trunk_price = {}
        self.distribution_price = {}

        for idx, row in self._trunk_infor.iterrows():
            origin = row['始发城市']
            destination = row['目的地城市']
            self.trunk_price[(origin, destination)] = {'weight_price': row['weight_price'],

                                                       }
        for idx, row in self._transfer_infor.iterrows():
            origin_b = row['始发城市']
            destination_b = row['目的地城市']
            self.distribution_price[(origin_b, destination_b)] = {
                                                               'base_weight_qty': row['base_weight_qty_toC'],
                                                               'base_price': row['base_price_toC'],
                                                               'weight_price_qty': row['weight_price_qty_toC'],
                                                                'distance': row['distance']
                                                               # 'time_median_toC': row['od_tm_mid_toC'],
                                                               # 'time_75_toC': row['od_tm_75_toC'],
                                                               # 'sla_toC': row['sla_toC'],
                                                               # 'base_weight_qty_toB': row['base_weight_qty_toB'],
                                                               # 'base_price_toB': row['base_price_toB'],
                                                               # 'weight_price_qty_toB': row['weight_price_qty_toB'],
                                                               # 'time_median_toB': row['od_tm_mid_toB'],
                                                               # 'time_75_toB': row['od_tm_75_toB'],
                                                               # 'sla_toB': row['sla_toB'],
                                                       }

    def _process_warehose_coeff(self):
        """
        deal with the warehouse coefficient of warehouse number

        :return:
        """
        self.warehouse_coeff = {}
        for idx, row in self._warehouse_coeff.iterrows():
            self.warehouse_coeff[row['仓个数']] = row['库存变化系数']

    def _process_demand(self):
        """
        deal with the demand of customer
        :return:
        """
        # future_ratio_c = self._config.future_ratio_c
        # future_ratio_b = self._config.future_ratio_b
        self.demand = {}
        self.customer = []

        # self.demand_2b = {}
        # self.customer_2b = []
        for idx, row in self._end_df_2c.iterrows():
            name = row['城市代码']
            self.customer.append(name)
            if name not in self.demand:
                self.demand[name] = {
                    'demand_sum':  row['总需求量'],
                    'demand_weight_sum':  row['总需求量重量'],
                    'SKU1':  row['SKU1'],
                    'SKU2':  row['SKU2'],
                    'SKU3':  row['SKU3'],
                    'SKU4':  row['SKU4'],
                    'SKU5':  row['SKU5'],
                    'SKU1_weight': row['SKU1_weight'],
                    'SKU2_weight': row['SKU2_weight'],
                    'SKU3_weight': row['SKU3_weight'],
                    'SKU4_weight': row['SKU4_weight'],
                    'SKU5_weight': row['SKU5_weight'],
                    # 'SKU6':  row['SKU6'],
                }

            self.customer = list(set(self.customer))
        # # toB
        # for idx, row in self._end_df_2b.iterrows():
        #     name_b = row['城市代码']
        #     self.customer_2b.append(name_b)
        #     if name_b not in self.demand_2b:
        #         self.demand_2b[name_b] = {
        #             'demand_sum': future_ratio_b * row['总需求量'],
        #             'demand_weight_sum': future_ratio_b * row['总需求量重量'],
        #             'replenishment': row['补货次数'],
        #         }
        #     elif name_b in self.demand_2b:
        #         self.demand_2b[name_b]['demand_sum'] += row['总需求量']
        #         self.demand_2b[name_b]['demand_weight_sum'] += row['总需求量重量']
        #
        #     self.customer_2b = list(set(self.customer_2b))

    def _process_gis(self):
        """
        deal with the gis of city
        :return:
        """
        self.city_add = {}
        for idx, row in self._city_gis.iterrows():
            self.city_add[row['城市代码']] ={'lgt': row['lgt'],
                                             'lat': row['lat'],
                                             'city_name': row['city_name'],
                                            "city" : row['城市名']
                                            }


class NetworkVisualization(object):
    """
    class which show the demand heat map and the allocation network
    """
    def __init__(self, data_class, json_file=None):
        """
        Initialization the class with data_class and json_file parameters, and add the customized city into Geo class
        :param data_class: the data_class
        :param json_file:
        """
        self.geo = Geo()
        self.data_class = data_class
        self.json_file = json_file
        if self.json_file is not None:
            self._load_coordinate_json()
        else:
            if not hasattr(self.data_class, 'city'):
                raise ValueError('the city and corresponding longitude and latitude should be included in data_class!')
            else:
                self._load_coordinate()

    def _load_coordinate(self):
        # add the customized city with the corresponding longitude and latitude
        if len(self.data_class.city) != len(self.data_class.longitude):
            raise ValueError('the length of city and longitude and latitude should be same !')
        for city, longitude, latitude in zip(self.data_class.city, self.data_class.longitude, self.data_class.latitude):
            self.geo.add_coordinate(city, longitude, latitude)

    def _load_coordinate_json(self):
        # add the customized city with the longitude and latitude json file to Geo
        # json file contents: { 'city name':[120.123,32.345] }
        self.geo.add_coordinate_json(self.json_file)

    def heat_map(self, city, demand) -> Geo:
        c = (
            Geo()
                .add_schema(maptype="china")
                .add("demand heat map ", [list(z) for z in zip(city, demand)],
                     type_=ChartType.HEATMAP,
                     symbol_size=8)
                .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
                .set_global_opts(
                visualmap_opts=opts.VisualMapOpts(max_=max(demand) * 1.2),
                title_opts=opts.TitleOpts(title="demand heat map "),
            )
        )
        return c

    def net_work_map(self, source_list, cost, destination_list, demand, source, destination) -> Geo:
        """
        output the network map based on the source and destination pair.
        :param source_list: the unique source list
        :param cost: the cost of source list
        :param destination_list: the unique destination list
        :param demand: the demand of destination
        :param source: the all source point
        :param destination: the all destination point
        :return:
        """
        c = (
            Geo()
                .add_schema(maptype="china")
                .add(
                "",
                [z for z in zip(source_list, cost)],
                type_=ChartType.SCATTER,
                color='green',
                symbol_size=5,
                symbol="image://..\\icon\\warehouse_1.png"
            )
                .add(
                "",
                [z for z in zip(destination_list, demand)],
                type_=ChartType.SCATTER,
                color='black',
                symbol_size=5,
                # symbol="image://..\\icon\\customer.png"
            )
                .add(
                "Network",
                [list(z) for z in zip(source, destination)],
                type_=ChartType.LINES,
                symbol=[None, 'arrow'],
                symbol_size=3,
                effect_opts=opts.EffectOpts(period=20, scale=1, symbol="image://..\\icon\\car.png",
                                            symbol_size=15, color="blue", trail_length=0),
                linestyle_opts=opts.LineStyleOpts(curve=0.1, opacity=0.3),
            )
                .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
                .set_global_opts(title_opts=opts.TitleOpts(title="Allocation Network"),
                                 visualmap_opts=opts.VisualMapOpts(is_piecewise=False, max_=max(demand),
                                                                   type_='size', range_size=[10, 20]))
        )
        return c


if __name__ == "__main__":
    import numpy as np
    filename = "model_input_HM_QG.xlsx"

    class Config(object):
        rdc_capacity = 23333

    data_ins = DataHandler(file=filename, config=Config)
    print(data_ins.trunk_price)
    print(data_ins.rdc_cand)
    print(data_ins.cdc_cand)
    print(data_ins.cdc_current)

    # class DataNet:
    #     pass
    #
    # city = []
    # longitude = []
    # latitude = []
    # for key, value in data_ins.city_add.items():
    #     city.append(key)
    #     longitude.append(value['lgt'])
    #     latitude.append(value['lat'])
    #
    # DataNet.city = city
    # DataNet.longitude = longitude
    # DataNet.latitude = latitude
    #
    # network = NetworkVisualization(DataNet)
    #
    # customer = []
    # demand = []
    # for name, value in data_ins.demand.items():
    #     customer.append(name)
    #     demand.append(value['demand_sum'])
    # heatmap = network.heat_map(customer, demand)
    # heatmap.render('demand heat-map.html')
    #
    # # read the network.csv file which contain the relationship between source and destination
    # df = pd.read_csv('../C_Network.csv', dtype=str)
    # df_rdc = pd.read_csv('../RDC.csv', dtype={'RDC': str})
    # source = df['RDC'].values.tolist()
    # source_lsit = df_rdc["RDC"].values.tolist()
    # destination = df['CUSTOMER'].values.tolist()
    # Volume = df_rdc['Volume'].values.tolist()
    # network = network.net_work_map(source_lsit, Volume, customer, demand, source, destination)
    # network.render("network of FangTai.html")
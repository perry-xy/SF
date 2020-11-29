#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
# @Time    : 2020/8/24 10:00
# @Author  : peng.wang
# @Email   : WangPeng4@sfmail.sf-express.com
# @FileName: main_wl.py
# @ProjectName :Facility_Location_WL
"""

from utils.util import DataHandler
from core.complex_model import FacilityLocation
from core.model import FacilityLocation
from core.cutoff_model import FacilityLocation
# from core.model import FacilityLocation
# from core.model_2 import FacilityLocation
from core.model_3 import FacilityLocation
from utils.misc import Logger
import pandas as pd
import os
# define the log file and log level
log = Logger(log_path='./log').logger
# define the configuration parameters


class Config(object):
    """
    define all parameters
    """
    # TODO: tune these capacity of cdc and rdc
    rdc_capacity = 5000000000000
    num_rdc = 1
    num_cdc = 4
    P_c = 0.95
    P_b = 0.8
    rr_cdc = 0.00
    weight_avg = 100
    future_ratio_c = 1.0         #0.87*7.14        # 2023 50百万
    future_ratio_b = 1.0            #1.2*1.2*1.2          # 2023 100百万
    # TODO: m^3/truck
    carton_volumn = 0.144
    area_ratio = 0.0
    ltr = 0.3
    upo = 1
    upc = 20    # 箱子装20件衣服
    upo_dist = 1
    turnover_days_c = 114/2                # 库存水平理论模型
    turnover_days_b = 52/2                 # 库存水平理论模型
    return_turnover_days = 365*2
    rr_c = 0.2
    rr_b = 0.1
    time_quantile_c = "time_75_toC"   #   time_75_toC , "time_median_toC", "time_95_toC"
    time_quantile_b = "time_median_toB"   # time_75_toB, "time_median_toB"

    category_list = ['SKU1', 'SKU2', 'SKU3', 'SKU4', 'SKU5']
    # enable status definition
    sla_2c_constrs_open = False
    sla_2b_constrs_open = False
    capacity_constrs_open = False
    current_location_constr_open = False
    use_location_constr_open = True

    sku_split = True
    dist_discount = 0.6
    warehouse_area_ratio = 2
    trunk_ratio = 0.2

# load the data
filename = "model_input_WL.xlsx"
data_ins = DataHandler(file=filename, config=Config)

df_performance = pd.DataFrame()
m = 0
# for num in range(37, 3, -1):

# for num in range(4,21):
for num in [10]:
# define the facility location model
    log.info('the model {} is running ....'.format(num))
    Config.num_rdc = num
    Config.area_ratio = data_ins.warehouse_coeff[num]
    location_model = FacilityLocation(data=data_ins, config=Config)
    model = location_model.facility_location()
    if model == 0:
        continue
    log.info('the model is end')

    # toC
    df_c_network = location_model.c_end_network()
    df_c_network.to_csv('C_Network_{}.csv'.format(num))

    if Config.sku_split:
        df_cdc_rdc_network = location_model.cdc_rdc_network()
        df_cdc_rdc_network.to_csv('CDC_RDC_Network_{}.csv'.format(num))
    #
    # df_tmp_c = df_c_network[['TIME', 'SLA', 'QUANTITY']].apply(lambda x:x[2] if (x[0]-x[1]) <= 0 else 0, axis=1)
    # df_tmp_time_c = df_c_network[['TIME', 'QUANTITY']].apply(lambda x: x[0] * x[1], axis=1)
    # SLA_2C = df_tmp_c.sum()/df_c_network['QUANTITY'].sum()
    # Quantity_time_avg_c = df_tmp_time_c.sum() / df_c_network['QUANTITY'].sum()
    # QUANTITY_c = df_c_network['QUANTITY'].sum()

    # toB
    # df_b_network = location_model.b_end_network()
    # df_b_network.to_csv('B_Network.csv')
    # df_tmp_b = df_b_network[['TIME', 'SLA', 'QUANTITY']].apply(lambda x: x[2] if (x[0] - x[1]) <= 0 else 0, axis=1)
    # df_tmp_time_b = df_b_network[['TIME', 'QUANTITY']].apply(lambda x: x[0] * x[1], axis=1)
    # SLA_2B = df_tmp_b.sum() / df_b_network['QUANTITY'].sum()
    # Quantity_time_avg_b = df_tmp_time_b.sum() / df_b_network['QUANTITY'].sum()
    # QUANTITY_b = df_b_network['QUANTITY'].sum()

    rdc_name_list = df_c_network['RDC'].unique()
    RDC_SLA = {}
    df_c_sla = pd.DataFrame()
    k = 0
    for rdc_name in rdc_name_list:
        # df = df_c_network[df_c_network['RDC'] == rdc_name][['TIME', 'SLA', 'QUANTITY']]\
        #     .apply(lambda x : x[2] if (x[0]-x[1]) <= 0 else 0, axis=1)
        # df_time = df_c_network[df_c_network['RDC'] == rdc_name][['TIME', 'QUANTITY']] \
        #     .apply(lambda x: x[0] * x[1], axis=1)

        # RDC_SLA['SLA'] = df.sum()/df_c_network[df_c_network['RDC'] == rdc_name]['QUANTITY'].sum()
        RDC_SLA['RDC'] = rdc_name
        RDC_SLA['CUSTOMER_NUM'] = len(df_c_network[df_c_network['RDC'] == rdc_name])
        # RDC_SLA['TIME_AVG'] = df_time.sum() / df_c_network[df_c_network['RDC'] == rdc_name]['QUANTITY'].sum()
        df_c_sla = df_c_sla.append(pd.DataFrame(RDC_SLA, index=[k]))
        k = k+1
    # TOB
    # rdc_name_list = df_b_network['RDC'].unique()
    # RDC_SLA = {}
    # df_b_sla = pd.DataFrame()
    # k = 0
    # for rdc_name in rdc_name_list:
    #     df = df_b_network[df_b_network['RDC']==rdc_name][['TIME', 'SLA', 'QUANTITY']]\
    #         .apply(lambda x:x[2] if (x[0]-x[1])<=0 else 0, axis=1)
    #     df_time = df_b_network[df_b_network['RDC'] == rdc_name][['TIME', 'QUANTITY']] \
    #         .apply(lambda x: x[0] * x[1], axis=1)
    #     RDC_SLA['SLA'] = df.sum()/df_b_network[df_b_network['RDC']==rdc_name]['QUANTITY'].sum()
    #     RDC_SLA['RDC'] = rdc_name
    #     RDC_SLA['CUSTOMER_NUM'] = len(df_b_network[df_b_network['RDC'] == rdc_name])
    #     RDC_SLA['TIME_AVG'] = df_time.sum() / df_b_network[df_b_network['RDC'] == rdc_name]['QUANTITY'].sum()
    #     df_b_sla = df_b_sla.append(pd.DataFrame(RDC_SLA, index=[k]))
    #     k = k+1

    df_cdc, cdc_shipping_cost = location_model.cdc_post_process()
    print(df_cdc)
    df_cdc.to_csv('CDC_{}.csv'.format(num))

    df_rdc, rdc_shipping_cost, rdc_storage_cost, rdc_inbound, rdc_outbound = location_model.rdc_post_process()
    print(df_rdc)
    print(df_c_sla)
    # print(df_b_sla)
    df_rdc = pd.merge(df_rdc, df_c_sla, on='RDC', suffixes=('', '_toC'), how='left')
    # df_rdc = pd.merge(df_rdc, df_b_sla, on='RDC', suffixes=('', '_toB'), how='left')
    df_rdc.to_csv('RDC_{}.csv'.format(num))



    # performance
    performance_output = {}
    performance_output['warehouse'] = Config.num_cdc + Config.num_rdc
    performance_output['CDC'] = Config.num_cdc
    performance_output['RDC'] = Config.num_rdc
    performance_output['Trans_cost'] = cdc_shipping_cost * Config.trunk_ratio
    performance_output['Dist_cost'] = rdc_shipping_cost * Config.dist_discount
    # performance_output['Dist_cost_toB'] = shipping_cost_b
    # performance_output['Dist_cost_toC'] = shipping_cost_c
    performance_output['Reverse_shipping_cost'] = 0
    performance_output['Storage_cost'] = rdc_storage_cost
    performance_output['Handling_cost'] = rdc_inbound + rdc_outbound
    # performance_output['Reverse_handling_cost'] = rdc_r_outbound
    # performance_output['Capital_cost'] = rdc_capital_cost
    performance_output['Total_Cost'] = performance_output['Trans_cost'] + performance_output['Dist_cost']\
                    + performance_output['Storage_cost'] \
                    + performance_output['Handling_cost']
                    # + performance_output['Capital_cost']
    # performance_output['SLA_2C'] = SLA_2C
    # performance_output['SLA_2B'] = SLA_2B
    # performance_output['TIME_AVG_2C'] = Quantity_time_avg_c
    # performance_output['PRICE_AVG_2C'] = performance_output['Total_Cost'] / QUANTITY_b
    # performance_output['TIME_AVG_2B'] = Quantity_time_avg_b
    # performance_output['PRICE_AVG_2B'] = performance_output['Total_Cost'] / QUANTITY_b
    # performance_output['DIST_PRICE_AVG_2C'] = performance_output['Dist_cost_toC'] / QUANTITY_c
    # performance_output['DIST_PRICE_AVG_2B'] = performance_output['Dist_cost_toB'] / QUANTITY_b
    df_performance = df_performance.append(pd.DataFrame(performance_output, index=[m]))
    m = m+1

df_performance.to_csv('performance.csv')






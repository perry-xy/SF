#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
# @Time    : 2020/11/17 16:44
# @Author  : peng.wang
# @Email   : WangPeng4@sfmail.sf-express.com
# @FileName: util.py
# @ProjectName :Prediction_Optimization
"""

import os
import pandas as pd
import xlwings as xw
from utils.misc import Logger

# define the log file
log = Logger(log_path='../log').logger
RAW_DATA_PATH = os.path.dirname(os.path.dirname(__file__))

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
        self._employee_process()
        self._demand_process()
        self._predict_samples_process()

    def _load_rawdata(self):
        """
        加载原始文件
        :return:
        """
        # another method for load excel data
        log.info('loading the excel file ......')
        app = xw.App(visible=False, add_book=False)
        wb = app.books.open(self._PATH)
        try:
            # '小哥信息表'
            self._employees = wb.sheets["小哥信息"].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                                          dtype=object).value
            # '预测信息'
            self._demands = wb.sheets["预测信息"].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                                          dtype=object).value
            self._demands['时间'] = pd.to_datetime(self._demands['时间']).dt.date
            # 预测样本区间
            self._predict_samples = wb.sheets['样本区间'].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                                          dtype=object).value
            wb.close()
            app.quit()
            log.info('loading the excel file over')
        except Exception as e:
            log.error('load the excel file error {}'.format(e))
            wb.close()
            app.quit()

    def _employee_process(self):
        """
        处理小哥的信息
        :return:
        """
        self.basic_cost = {}     # dict
        self.employees = []      # list
        self.capacity = {}       # dict
        self.capacity_cost = {}  # dict
        for idx, row in self._employees.iterrows():
            name_id = row['小哥id']
            self.employees.append(name_id)
            if name_id not in self.basic_cost:
                self.basic_cost[name_id] = row['保底收入']
                self.capacity[name_id] = {'receive': row['收件能力'],
                                          'send': row['派件能力'],
                                          }
                self.capacity_cost[name_id] = {'receive': row['收件成本'],
                                               'send': row['派件成本'],
                                               }

    def _demand_process(self):
        """
        进行需求预测的处理
        :return:
        """
        self.zones = []     # list
        self.demands = {}   # dict
        time_list = self._demands['时间'].unique()
        self.zones = self._demands['区域id'].unique().tolist()
        for t in time_list:
            demands_dict = {}
            tmp_df = self._demands[self._demands['时间'] == t]
            for idx, row in tmp_df.iterrows():
                zone_id = row['区域id']
                demands_dict[zone_id] = {'receive':row['收件量'],
                                         'send': row['派件量'],
                                         'receive_predict': row['收件量预测'],
                                         'send_predict': row['派件量预测']}
            self.demands[t] = demands_dict     # key：日期，value：dict：key：zone_id，value：receive,send,receive_predict,send_predict

    def _predict_samples_process(self):
        """

        :return:
        """
        self.receive_samples = {}
        self.send_samples = {}
        time_list = self._demands['时间'].unique()
        for t in time_list:
            tmp_df = self._predict_samples[self._predict_samples['时间'] == t]
            col_num = len(tmp_df.columns.values)
            self.receive_samples[t] = tmp_df.iloc[:, 1:int(col_num/2)+1]
            self.send_samples[t] = tmp_df.iloc[:, [1]+list(range(int(col_num/2)+1, int(col_num)))]


        #     for zone in self.zones:
        #         tmp_ddf = tmp_df[tmp_df['区域id']==zone]
        #         receive_value = tmp_ddf['收件量'].values.tolist()
        #         send_value = tmp_ddf['派件量'].values.tolist()
        #         self.demands.update({t: {zone: {'receive': receive_value, 'send': send_value}}})

def CVaR(data, alpha):
    data.sort()
    n = len(data)
    m = int(alpha * n)
    return sum(data[m:-1]) / len(data[m:-1])

if __name__ == "__main__":

    # define the configuration parameters
    class Config(object):
        Dates = []
        mode = 'deterministic'
        # target = 'reality'
        target = 'predict'
        level = '90_var'

    # load the data
    filename = "data_input_level.xlsx"
    data_ins = DataHandler(file=filename, config=Config)
    print(data_ins.send_samples)
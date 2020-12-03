#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
# @Time    : 2020/11/17 18:57
# @Author  : peng.wang
# @Email   : WangPeng4@sfmail.sf-express.com
# @FileName: main.py
# @ProjectName :Prediction_Optimization
"""


from utils.util import DataHandler
from utils.misc import Logger
from core.model import Scheduler
import pandas as pd
import os

# define the log file and log level
log = Logger(log_path='./log').logger


# define the configuration parameters
class Config(object):
    Dates = []


# load the data
filename = "data_input.xlsx"
data_ins = DataHandler(file=filename, config=Config)

print(data_ins.demands)

for key in data_ins.demands.keys():
    print(key)
    data_ins.demands_daily = (data_ins.demands[key])
    scheduler = Scheduler(Config, data_ins)
    model = scheduler.resource_scheduler_deterministic()
    solution, employee_num = scheduler.scheduler_solution()
    print(solution, employee_num)
    total_cost, total_basic_cost, total_payment, total_discount_payment = scheduler.total_cost()
    print(total_cost, total_basic_cost, total_payment, total_discount_payment)
    result_df = scheduler.scheduler_result()
    result_df.to_excel('results.xlsx')
    break

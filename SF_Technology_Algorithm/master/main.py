#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
# @Time    : 2020/11/17 18:57
# @Author  : peng.wang
# @Email   : WangPeng4@sfmail.sf-express.com
# @FileName: main.py
# @ProjectName :Prediction_Optimization
"""
import os
import json
from utils.util import DataHandler
from utils.misc import Logger
from core.model import Scheduler
from utils.sensitivity_analysis import Sensitivity
from  datetime import  datetime

# define the log file and log level
log = Logger(log_path='./log').logger


# define the configuration parameters
class Config(object):
    Dates = []

if not os.path.exists('results'):
    os.mkdir('results')

# load the data
filename = "data_input.xlsx"
data_ins = DataHandler(file=filename, config=Config)
total_cost_dict = {}
result_solution = {}
delta = [-9]
for i in delta:
    for key in data_ins.demands.keys():
        data_ins.demands_daily = (data_ins.demands[key])
        for k,v in data_ins.demands_daily.items():
            data_ins.demands_daily[k]['send']+=i
        scheduler = Scheduler(Config, data_ins)
        model = scheduler.resource_scheduler_deterministic()
        solution, employee_num = scheduler.scheduler_solution()
        total_cost, total_basic_cost, total_payment, total_discount_payment = scheduler.total_cost()
        result_df = scheduler.scheduler_result()
        # result_df.to_excel('results/results_{}'.format(key.strftime('%Y-%m-%d'))+str(i)+'.xlsx')
        result_solution[key.strftime('%Y-%m-%d')] = dict(solution)
        total_cost_dict[key] = total_cost
    print(total_cost_dict)
    # with open("results/solution"+str(i)+".json", "w") as fp:
    #     json.dump(result_solution, fp)
# sensitivity = Sensitivity(data_ins, r'.\results\solution.json')
# daily_cost_dict = sensitivity.disturb()


# fp.close()


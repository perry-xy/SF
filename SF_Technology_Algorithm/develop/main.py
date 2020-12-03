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
mode = 'deterministic'
mode = 'expected'
# mode = 'expected_cvar'

if mode == 'deterministic':
    result_solution = {}
    for key in data_ins.demands.keys():
        data_ins.demands_daily = (data_ins.demands[key])
        scheduler = Scheduler(Config, data_ins)
        model = scheduler.resource_scheduler_deterministic()
        solution, employee_num = scheduler.scheduler_solution()
        total_cost, total_basic_cost, total_payment, total_discount_payment = scheduler.total_cost()
        result_df = scheduler.scheduler_result()
        result_df.to_excel('results/results_{}.xlsx'.format(key.strftime('%Y-%m-%d')))
        result_solution[key.strftime('%Y-%m-%d')] = dict(solution)
    with open("results/solution_{}.json".format(mode), "w") as fp:
        json.dump(result_solution, fp)

    fp.close()
elif mode =='expected':
    result_solution = {}
    for key in data_ins.demands.keys():
        data_ins.demands_daily = (data_ins.demands[key])
        scheduler = Scheduler(Config, data_ins)
        model = scheduler.resource_scheduler_expected()
        solution, employee_num = scheduler.scheduler_solution()
        # total_cost, total_basic_cost, total_payment, total_discount_payment = scheduler.total_cost()
        # result_df = scheduler.scheduler_result()
        # result_df.to_excel('results/results_{}.xlsx'.format(key.strftime('%Y-%m-%d')))
        result_solution[key.strftime('%Y-%m-%d')] = dict(solution)
    with open("results/solution_{}.json".format(mode), "w") as fp:
        json.dump(result_solution, fp)

    fp.close()

elif mode =='expected_cvar':
    result_solution = {}
    for key in data_ins.demands.keys():
        log.info('running the model based on {} predicted results'.format(key.strftime('%Y-%m-%d')))
        data_ins.demands_daily = (data_ins.demands[key])
        scheduler = Scheduler(Config, data_ins)
        model = scheduler.resource_scheduler_expected_cvar()
        solution, employee_num = scheduler.scheduler_solution()
        # total_cost, total_basic_cost, total_payment, total_discount_payment = scheduler.total_cost()
        # result_df = scheduler.scheduler_result()
        # result_df.to_excel('results/results_{}.xlsx'.format(key.strftime('%Y-%m-%d')))
        result_solution[key.strftime('%Y-%m-%d')] = dict(solution)
    with open("results/solution_{}.json".format(mode), "w") as fp:
        json.dump(result_solution, fp)

    fp.close()





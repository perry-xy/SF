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
import pandas as pd
from utils.util import DataHandler
from utils.misc import Logger
from core.model import Scheduler
from utils.sensitivity_analysis import Sensitivity
# define the log file and log level
log = Logger(log_path='./log').logger


# define the configuration parameters
class Config(object):
    Dates = []
    mode = 'deterministic'
    # target = 'reality'
    target = 'predict'
    level = '12_02_80var_var'

if not os.path.exists('results'):
    os.mkdir('results')

# load the data
filename = "data_input_level_80cvar.xlsx"
data_ins = DataHandler(file=filename, config=Config)
# mode = 'initial'
mode = 'deterministic'
# target = 'reality'
target = 'predict'
# mode = 'expected'
# mode = 'expected_cvar'
solution_path = "results/solution_{}_{}_{}.json".format(mode, Config.target, Config.level)
if mode == 'deterministic':
    result_solution = {}
    for key in data_ins.demands.keys():  # key：日期
        data_ins.demands_daily = (data_ins.demands[key])
        scheduler = Scheduler(Config, data_ins)
        key = pd.to_datetime(key)
        log.info(key)
        model = scheduler.resource_scheduler_deterministic()
        solution, employee_num = scheduler.scheduler_solution()
        total_cost, total_basic_cost, total_payment, total_discount_payment = scheduler.total_cost()
        result_df = scheduler.scheduler_result()
        # result_df.to_excel('results/results_{}_{}_{}_{}.xlsx'.format(key.strftime('%Y-%m-%d'), mode, Config.target, Config.level))
        result_solution[key.strftime('%Y-%m-%d')] = dict(solution)
    with open(solution_path, "w") as fp:
        json.dump(result_solution, fp)
    fp.close()

elif mode == 'initial':
    result_solution = {}
    for key in data_ins.demands.keys():
        data_ins.demands_daily = (data_ins.demands[key])
        data_ins.send_samples_daily = (data_ins.send_samples[key])
        data_ins.receive_samples_daily = (data_ins.receive_samples[key])
        scheduler = Scheduler(Config, data_ins)
        model = scheduler.resource_scheduler_initial()
        solution, employee_num = scheduler.scheduler_solution()
        # total_cost, total_basic_cost, total_payment, total_discount_payment = scheduler.total_cost()
        # result_df = scheduler.scheduler_result()
        # result_df.to_excel('results/results_{}_{}.xlsx'.format(key.strftime('%Y-%m-%d'), mode))
        result_solution[key.strftime('%Y-%m-%d')] = dict(solution)
    with open(solution_path, "w") as fp:
        json.dump(result_solution, fp)

    fp.close()
elif mode == 'expected':
    result_solution = {}
    for key in data_ins.demands.keys():
        data_ins.demands_daily = (data_ins.demands[key])
        data_ins.send_samples_daily = (data_ins.send_samples[key])
        data_ins.receive_samples_daily = (data_ins.receive_samples[key])
        scheduler = Scheduler(Config, data_ins)
        model = scheduler.resource_scheduler_expected()
        solution, employee_num = scheduler.scheduler_solution()
        # total_cost, total_basic_cost, total_payment, total_discount_payment = scheduler.total_cost()
        # result_df = scheduler.scheduler_result()
        # result_df.to_excel('results/results_{}.xlsx'.format(key.strftime('%Y-%m-%d')))
        result_solution[key.strftime('%Y-%m-%d')] = dict(solution)
    with open(solution_path, "w") as fp:
        json.dump(result_solution, fp)

    fp.close()

elif mode == 'expected_cvar':
    result_solution = {}
    for key in data_ins.demands.keys():
        log.info('running the model based on {} predicted results'.format(key.strftime('%Y-%m-%d')))
        data_ins.demands_daily = (data_ins.demands[key])
        data_ins.send_samples_daily = (data_ins.send_samples[key])
        data_ins.receive_samples_daily = (data_ins.receive_samples[key])
        scheduler = Scheduler(Config, data_ins)
        model = scheduler.resource_scheduler_expected_cvar()
        solution, employee_num = scheduler.scheduler_solution()
        # total_cost, total_basic_cost, total_payment, total_discount_payment = scheduler.total_cost()
        # result_df = scheduler.scheduler_result()
        # result_df.to_excel('results/results_{}.xlsx'.format(key.strftime('%Y-%m-%d')))
        result_solution[key.strftime('%Y-%m-%d')] = dict(solution)
    with open(solution_path, "w") as fp:
        json.dump(result_solution, fp)

    fp.close()






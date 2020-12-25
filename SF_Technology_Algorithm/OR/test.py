#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
# @Time    : 2020/11/23 18:48
# @Author  : peng.wang
# @Email   : WangPeng4@sfmail.sf-express.com
# @FileName: test.py
# @ProjectName :competition
"""
from utils.sensitivity_analysis import Sensitivity
from utils.util import DataHandler


# define the configuration parameters
class Config(object):
    Dates = []
# load the data


filename = "data_input_level_80cvar.xlsx"
data_ins = DataHandler(file=filename, config=Config)
mode = 'deterministic'
target = 'reality'
# target = 'predict'
level = '12_02_reality'
# mode = 'expected'
# mode = 'expected_cvar'
test_mode = 'optimized'
test_mode = 'reality'
solution_path = "results/solution_{}_{}_{}.json".format(mode, target, level)
optimized_json_path = "results/best_strategy.json"

# Sensitivity analysis
if test_mode=='optimized':
    sa = Sensitivity(data_ins, optimized_json_path)
    sa.disturb(mode, target,test_mode)
else:
    sa = Sensitivity(data_ins, solution_path)
    sa.disturb(mode, target,level)


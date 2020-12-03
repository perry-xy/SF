'''
# @Time        :2020/11/19 16:14
# @Author      :ChunRong.Chen
# @ FileName   :test.py  
'''
from utils.sensitivity_analysis import Sensitivity
from utils.util import DataHandler
class Config(object):
    Dates = []


# load the data
filename = "data_input.xlsx"
data_ins = DataHandler(file=filename, config=Config)
sensitivity = Sensitivity(data_ins, r'.\results\solution.json')
daily_cost_dict = sensitivity.disturb()
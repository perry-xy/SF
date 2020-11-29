#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
# @Time    : 2019/11/6 10:57
# @Author  : peng.wang
# @Email   : WangPeng4@sfmail.sf-express.com
# @FileName: main.py
# @ProjectName :sh-demand-forecast-alg
"""
import os
import yaml
import warnings
import pandas as pd
import numpy as np
from core import data_handle
from model.stlforcator_train import STLForecastorTrain
from utils.misc import Logger
from utils.util import GetExternalData

warnings.filterwarnings("ignore")
log = Logger(log_path=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'log')).logger

with open(os.path.join(os.path.dirname(__file__), 'config/config.yaml')) as fp:
    config = yaml.load(fp)


def main(model_name):
    """
    main function for run the offline training and validation process, output the train error
    if the cross-validation selected, then give the cross-validation results
    :param model_name:
    :return:
    """
    # load data
    id = '1197'
    data_path = config['DATA']['data_path']
    time_column = config['DATA']['time_column']
    id_column = config['DATA']['id_column']
    target_column = config["DATA"]['target_column']

    #获取天气、假期等外部数据
    #ExternalData = GetExternalData(start_date='2018-01-01', end_date='2019-12-31')
    #df_external = ExternalData.get_external()

    data_ins = data_handle.DataHandler(data_path=data_path, time_column=time_column, id_column=id_column,
                                       target_column=target_column, multiple_sku=True, logger=log)
    data_dict = data_ins.df_by_id
    df = data_dict[id]
    df = df[df[time_column] >= '2018-01-01'] #只取2018年1月1号之后的数据，总共的数据为：2018.0101~2019.1028
    df.reset_index(inplace=True, drop=True)

    cv_setting = {'horizon': '7days', 'period': '7days', "train_end_date": '2019-06-01'}
    for model in model_name:
        forecastor = STLForecastorTrain(logger=log)
        forecastor.model_setup(model_name=model, save_model=True, feature_selection=False) #存储Model
        forecastor.data_setup(df, target_column, id_column, time_column, with_external=False, #external_df=df_external,
                              external_column=['holiday'], forecast_period='1week') #周度预测，模型准备
        result_dict = forecastor.build_model(save_feature=True, **cv_setting) #规定了horizon、period和训练集
        # log.info('result_df:')
        # log.info(result_df)

        result_df = result_dict['train_result']
        result_df['time'] = result_dict['time']
        result_df.to_csv('train_results.csv')
        result_dict['feature'].to_csv('feature.csv')


if __name__ == "__main__":
    main(model_name=['XGB'])


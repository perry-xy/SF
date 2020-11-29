#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
# @Time    : 2019/8/29 18:15
# @Author  : peng.wang
# @Email   : WangPeng4@sfmail.sf-express.com
# @FileName: misc.py
# @ProjectName :Storage-Planning
"""

import os
import time
import sys
import warnings
import logging
import numpy as np
import pandas as pd
from sklearn.externals import joblib
from config.config import FORECAST_1_WEEK_VALUE, FORECAST_1_MONTH_VALUE
# from singleton import Singleton
warnings.filterwarnings("ignore")


# @Singleton  # 如需打印不同路径的日志（运行日志、审计日志），则不能使用单例模式（注释或删除此行）。此外，还需设定参数name。
class Logger:
    def __init__(self, set_level="INFO",
                 name=os.path.split(os.path.splitext(sys.argv[0])[0])[-1],
                 log_name=os.path.split(os.path.splitext(sys.argv[0])[0])[-1]+'_'+time.strftime("%Y-%m-%d.log", time.localtime()),
                 log_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), "log"),
                 use_console=True):
        """
        :param set_level: 日志级别["NOTSET"|"DEBUG"|"INFO"|"WARNING"|"ERROR"|"CRITICAL"]，默认为INFO
        :param name: 日志中打印的name，默认为运行程序的name
        :param log_name: 日志文件的名字，默认为当前时间（年-月-日.log）
        :param log_path: 日志文件夹的路径，默认为logger.py同级目录中的log文件夹
        :param use_console: 是否在控制台打印，默认为True
        """
        if not set_level:
            set_level = self._exec_type()  # 设置set_level为None，自动获取当前运行模式
        self.__logger = logging.getLogger(name)
        self.setLevel(
            getattr(logging, set_level.upper()) if hasattr(logging, set_level.upper()) else logging.INFO)  # 设置日志级别
        if not os.path.exists(log_path):  # 创建日志目录
            os.makedirs(log_path)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        if not self.__logger.handlers:
            handler_list = list()
            handler_list.append(logging.FileHandler(os.path.join(log_path, log_name), encoding="utf-8"))
            if use_console:
                handler_list.append(logging.StreamHandler())
            for handler in handler_list:
                handler.setFormatter(formatter)
                self.addHandler(handler)

    def __getattr__(self, item):
        return getattr(self.logger, item)

    @property
    def logger(self):
        return self.__logger

    @logger.setter
    def logger(self, func):
        self.__logger = func

    def _exec_type(self):
        return "DEBUG" if os.environ.get("IPYTHONENABLE") else "INFO"


def mean_abs_percentage_error(actual, pred):
    # in the case there are some 0 values in actual
    valid_index = np.where(actual > 0)[0]
    return np.average(np.abs(actual[valid_index] - pred[valid_index]) / actual[valid_index]) * 100


def symmetric_mean_abs_percentage_error(actual, pred):
    """

    :param actual:  dtype: array
    :param pred:    dtype: array
    :return:
    """
    # in the case there are some 0 values in actual
    valid_index = np.where(actual > 0)[0]
    return np.average(
        200.0 * np.abs(actual[valid_index] - pred[valid_index]) / (pred[valid_index] + actual[valid_index])) * 100


def mean_square_percentage_error(actual, pred):
    valid_index = np.where(actual > 0)[0]
    return np.sqrt(np.average(np.power(np.abs(actual[valid_index] - pred[valid_index]) / actual[valid_index], 2))) * 100


def xgb_mape(pred, DMatrix):
    target = DMatrix.get_label()
    return "mse_mape", mean_abs_percentage_error(target, pred)


def evaluate_mape(target_df, pred_df):
    df = pd.merge(target_df, pred_df, on="date_time", how="left")
    df.dropna(axis=0, inplace=True)
    error_value = mean_abs_percentage_error(df.target, df.predict_target)
    return error_value


def map_forecast_periods(forecast_period_str):
    if forecast_period_str == FORECAST_1_MONTH_VALUE:
        return 30
    elif forecast_period_str == FORECAST_1_WEEK_VALUE:
        return 7
    else:
        return 1


def save_model_to_file(model, model_path):
    joblib.dump(model, model_path)


def load_model_from_file(model_path):
    return joblib.load(model_path)


# def load_holiday_data(holiday_data_path, location):
#     df_holiday = pd.read_csv(holiday_data_path)
#     df_holiday.date_time = pd.to_datetime(df_holiday.date_time)
#     df_holiday = df_holiday[df_holiday.location == location]
#     if df_holiday.empty:
#         logging.ERROR("Holiday data doesnt match the location! No holiday data is used!")
#     else:
#         df_holiday.drop('location', axis=1, inplace=True)
#         return df_holiday


# def map_forecast_periods(forecast_period_str):
#     if forecast_period_str == FORECAST_1_MONTH_VALUE:
#         return 30
#     elif forecast_period_str == FORECAST_1_WEEK_VALUE:
#         return 7
#     else:
#         return 1




if __name__ == '__main__':
    log = Logger(log_path='../log')
    log.logger.info('test')

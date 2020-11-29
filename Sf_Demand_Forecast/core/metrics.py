#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
# @Time    : 2019/10/30 16:32
# @Author  : peng.wang
# @Email   : WangPeng4@sfmail.sf-express.com
# @FileName: metrics.py
# @ProjectName :sh-demand-forecast-alg
"""
import numpy as np


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
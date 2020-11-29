# -*- coding: utf-8 -*-
"""
Created on Thu Nov  5 16:29:39 2020

@author: Administrator
"""

import lightgbm as lgb
from hyperopt import hp
import numpy as np

class Config():
    """
    模型特征参数
    """
    feature_start_index = 3
    feature_loop_index = 3
    one_category_feature_num = 34
    default_params = lgb.LGBMRegressor().get_params()
    params_optimization = True
    space = {
        'boosting_type': hp.choice('boosting_type',
                                   [{'boosting_type': 'gbdt', 'subsample': hp.uniform('gdbt_subsample', 0.5, 1)},
                                    {'boosting_type': 'dart', 'subsample': hp.uniform('dart_subsample', 0.5, 1)},
                                    {'boosting_type': 'goss', 'subsample': 1.0}]),
        'num_leaves': hp.quniform('num_leaves', 20, 150, 1),
        'learning_rate': hp.loguniform('learning_rate', np.log(0.01), np.log(0.5)),
        #'subsample_for_bin': hp.quniform('subsample_for_bin', 20000, 300000, 20000),
        #'min_child_samples': hp.quniform('min_child_samples', 20, 500, 5),
        'reg_alpha': hp.uniform('reg_alpha', 0.0, 1.0),
        'reg_lambda': hp.uniform('reg_lambda', 0.0, 1.0),
        'colsample_bytree': hp.uniform('colsample_by_tree', 0.6, 1.0),
        #'is_unbalance': hp.choice('is_unbalance', [True, False]),
        'max_depth': hp.quniform('max_depth', 4, 10, 1)
    }  #贝叶斯优化的搜索空间
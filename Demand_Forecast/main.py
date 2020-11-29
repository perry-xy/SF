# -*- coding: utf-8 -*-
"""
Created on Thu Nov  5 21:29:39 2020

@author: Administrator
"""
from core.featureEngineer import FeatureEngineer
from core.datahandle import DataHandler
from model.model import LGB_Model
from hyperopt import tpe
from hyperopt import Trials
from hyperopt import fmin
import csv
from core.config import Config
import lightgbm as lgb
import pandas as pd

filepath = 'data'

handled_unfeatured_data = DataHandler(filepath)
unfeatured_data = FeatureEngineer(handled_unfeatured_data.sales_per_month, Config)

train_data = unfeatured_data.feature_transform(start_index = 0,
                                               end_index = Config.one_category_feature_num - Config.feature_loop_index,
                                               data_type = 'train')
test_data = unfeatured_data.feature_transform(start_index = Config.one_category_feature_num - Config.feature_loop_index,
                                               end_index = Config.one_category_feature_num - Config.feature_loop_index +1,
                                               data_type = 'test')  # data to be predicted

# 用默认参数构建预测baseline
lgb_model = LGB_Model(Config, train_data, test_data, task_type='Regression')
base_iterations, base_predict_error, default_model = lgb_model.model_train(metric = 'mape') #所用参数见default_params.yaml；此处为学习器个数及预测误差

# 对所有数据预测
# X = train_data.drop('target', axis=1)
# all_X_predict = default_model.predict(X.values)
# all_X_predict = pd.Series(all_X_predict)
# all_test_result = pd.concat([train_data, all_X_predict], axis = 1)
# all_test_result = list(train_data.columns) + ['predict']
# all_test_result = all_test_result[['shop_id','item_id','item_category_id','year','month','target','predict']]
# all_test_result.to_csv('default_all_test_result.csv', encoding = 'utf-8_sig')
print('开始进行贝叶斯优化。')

# 贝叶斯参数优化
if Config.params_optimization == True:

    lgb_model.iterations = 0
    lgb_model.out_file = 'bayes_test.csv'

    trials = Trials()
    of_connection = open(lgb_model.out_file, 'w')
    writer = csv.writer(of_connection)
    # Write column names
    headers = ['loss', 'hyperparameters', 'iteration', 'status']
    writer.writerow(headers)
    of_connection.close()

    best = fmin(fn = lgb_model.bayes_optimize_objective, space = Config.space, algo = tpe.suggest,
                                                        trials = trials, max_evals = 10)
    print('最佳参数为：\n')
    print(best)
    print('最佳结果为: \n')
    trials_dict = sorted(trials.results, key=lambda x: x['loss'])
    print(trials_dict[:1])

    results = pd.read_csv(lgb_model.out_file)
    bayes_params, hyp_df = lgb_model.evaluate(results, name = 'LGBModel', metric = 'mape')
    print(hyp_df)
    evals_result = {}
    bayes_model = lgb.train(params = bayes_params,
                            train_set = lgb_model.dtrain,
                            feature_name = lgb_model.feature_name,
                            valid_sets = lgb_model.valid_sets,
                            valid_names = lgb_model.valid_name,
                            evals_result = evals_result,
                            early_stopping_rounds = 0.1 * bayes_params['n_estimators'] )


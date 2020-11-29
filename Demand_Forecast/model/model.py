# -*- coding: utf-8 -*-
"""
Created on Thu Nov  5 21:29:39 2020

@author: Administrator
"""
from sklearn.model_selection import train_test_split
import lightgbm as lgb
import matplotlib.pyplot as plt
import csv
from hyperopt import STATUS_OK
import ast
import pandas as pd

class LGB_Model():
    """
    Regression / Classifier model #TODO：the classifiler model
    """
    def __init__(self, config, train_data, test_data, task_type = 'Regression'):
        """
        共有变量及属性
        :param task_type:
        """
        self.config = config
        self.train_data = train_data
        self.test_data = test_data
        self.task_type = task_type
        self.default_params = self.config.default_params
        self.dtrain = None  #lgb Dataset
        self.dtest = None   #lgb Dataset
        self.feature_name = list()
        self.valid_sets = None
        self.valid_name = ['train', 'eval']
        self.iterations = None #优化次数记录器
        self.out_file = None #优化结果写入文件
        self.transform_data()

    def transform_data(self):
        """
        transform the data to dtrain, dtest
        :return:
        """
        y = self.train_data['target']
        X = self.train_data.drop('target', axis=1)
        print(X.info())

        self.feature_name = list(X.columns)
        train_X, test_X, train_y, test_y = train_test_split(
                                            X, y, test_size=0.3, random_state=123)

        self.dtrain = lgb.Dataset(train_X.values, label=train_y.values)
        self.dtest = lgb.Dataset(test_X.values, label=test_y.values)

        self.valid_sets = [self.dtrain, self.dtest]

    def model_train(self, metric = 'mape'):
        """
        model to construct the predict baseline
        :param train_data:
        :return:
        """
        default_params = self.default_params

        # 构造训练baseline
        evals_result = {}
        default_params['metric'] = [metric]
        default_params['objective'] = 'regression'
        early_stop = int(0.1 * default_params['n_estimators'])
        model = lgb.train(params = default_params,
                          train_set = self.dtrain,
                          feature_name = self.feature_name,
                          valid_sets = self.valid_sets,
                          valid_names = self.valid_name,
                          evals_result = evals_result,
                          early_stopping_rounds = early_stop
                          )
        fig, ax = plt.subplots()
        ax.plot(evals_result['train'][metric], label='Train')
        ax.plot(evals_result['eval'][metric], label='Test')
        ax.legend()
        plt.ylabel(f'{metric}')
        plt.title(f'lightGBM {metric}')
        plt.show()
        if len(evals_result['eval'][metric]) == default_params['n_estimators']:
            iterations = default_params['n_estimators']
            default_mape = evals_result['eval'][metric][-1]
        else: #TODO:confirm the best_itreations
            iterations = model.best_iteration
            default_mape = evals_result['eval'][metric][iterations -1]

        print('默认参数下LGB在测试集上的迭代次数为{}次，预测的{}为:{}'.format(str(iterations), metric, str(default_mape)))

        return iterations, default_mape, model

    def bayes_optimize_objective(self, hyperparameters):
        """
        定义贝叶斯优化目标函数，需返回递减的loss，或'loss'键后面是递减的score
        :param hyperparameters:
        :return:
        """
        self.iterations += 1
        metric = 'mape'

        if 'n_estimators' in hyperparameters:
            del hyperparameters['n_estimators']

        subsample = hyperparameters['boosting_type'].get('subsample', 1.0)  # 若没有subsample这个键时，返回1.0

        hyperparameters['boosting_type'] = hyperparameters['boosting_type']['boosting_type']
        hyperparameters['subsample'] = subsample

        for parameter_name in ['num_leaves', 'max_depth']:  # ['num_leaves', 'subsample_for_bin', 'min_child_samples']:
            hyperparameters[parameter_name] = int(hyperparameters[parameter_name])

        hyperparameters['metric'] = [metric]

        evals_result = {}
        opti_results = lgb.train(hyperparameters,
                                   self.dtrain,
                                   num_boost_round = 500,
                                   early_stopping_rounds = 20,
                                   feature_name = self.feature_name,
                                   valid_sets = self.valid_sets,
                                   valid_names = self.valid_name,
                                   evals_result = evals_result,
                                   )
        if len(evals_result) == 500: #TODO:best_score
            loss = evals_result['eval'][metric][-1]
        else:
            loss = evals_result['eval'][metric][opti_results.best_iteration -1]

        n_estimators = len(evals_result['eval'][metric])

        hyperparameters['n_estimators'] = n_estimators

        of_connection = open(self.out_file, 'a')
        writer = csv.writer(of_connection)
        writer.writerow([loss, hyperparameters, self.iterations, STATUS_OK])
        of_connection.close()

        return {'loss': loss, 'hyperparameters': hyperparameters, 'iteration': self.iterations, 'status': STATUS_OK}

    def evaluate(results, name, metric = 'mape'):

        new_results = results.copy()
        new_results['hyperparameters'] = new_results['hyperparameters'].map(ast.literal_eval)

        new_results = new_results.sort_values('loss', ascending=True).reset_index(drop=True)

        print('The best perform from {} was {:.5f} found on iteration {}.'.format(name,
                                                                                 new_results.loc[0, 'score'],
                                                                                 new_results.loc[0, 'iteration']))

        hyperparameters = new_results.loc[0, 'hyperparameters']
        #model = lgb.LGBMRegressor(**hyperparameters)

        #model.fit(train_features, train_labels)
        #preds = model.predict_proba(test_features)[:, 1]

        #print('ROC AUC from {} on test data = {:.5f}.'.format(name, roc_auc_score(test_labels, preds)))

        hyp_df = pd.DataFrame(columns=list(new_results.loc[0, 'hyperparameters'].keys()))

        for i, hyp in enumerate(new_results['hyperparameters']):
            hyp_df = hyp_df.append(pd.DataFrame(hyp, index=[0]),
                                   ignore_index=True)

        hyp_df['iteration'] = new_results['iteration']
        hyp_df['metric'] = [metric]

        return hyperparameters, hyp_df






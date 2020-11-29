#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
# @Time    : 2019/10/8 21:37
# @Author  : peng.wang
# @Email   : WangPeng4@sfmail.sf-express.com
# @FileName: model.py
# @ProjectName :sh-demand-forecast-alg
"""
import os
import warnings
import itertools
import pandas as pd
import numpy as np
from sklearn.base import BaseEstimator
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import DotProduct, WhiteKernel
from sklearn.svm import SVR
from sklearn.linear_model import Lasso, Ridge
from sklearn.model_selection import GridSearchCV
#
from utils.misc import Logger
from utils.util import generate_cutoffs
from utils.misc import save_model_to_file, mean_abs_percentage_error, xgb_mape

log = Logger(log_path=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'log')).logger
warnings.filterwarnings("ignore")

xgb_installed = False
lgt_installed = False
try:
    import xgboost as xgb
    xgb_installed = True
    import lightgbm as lgt
    lgt_installed = True
except ImportError:
    pass


class TrainModel(BaseEstimator):

    num_round = 30

    def __init__(self, model_name=None, model_param=None, param_optimize=False, target_column='target', key_columns=[],
                 save_model=False, model_path="", data_interval=1, logger=log):
        self.param_optimize = param_optimize
        self.model_name = model_name
        self.model_param = model_param
        self.valid_set = None
        self.df = pd.DataFrame()
        self.target_column = target_column
        self.predict_target_column = "pred_" + target_column
        self.key_columns = key_columns
        self.row_sample = True
        self.grid_param = None
        self.predict_result = None
        self.save_model = save_model
        self.model_path = model_path
        self.data_interval = data_interval
        self.logger = logger

    def fit(self, df):
        return self

    def transform(self, data_dict):

        # get the training data
        target_array = data_dict["target"]
        feature_df = data_dict["feature"]

        if self.logger:
            self.logger.info("...TrainMLmodel...")
            self.logger.info("......Features:")
            self.logger.info(feature_df.columns)

        # parameter setup
        self.model = self.get_model_by_name(self.model_name)
        if self.model_param is not None:
            if self.logger:
                self.logger.info("Training based on parameters")
                self.logger.info(self.model_param)
            self.model = self.model.set_params(**self.model_param)
        else:
            if self.logger:
                self.logger.info("Training based on the default parameters")
            default_param = self.get_model_default_param(self.model_name)
            self.model = self.model.set_params(**default_param)
            self.logger.info(default_param)

        if self.model_name in ['XGB', 'LGB']:
            # remove valid set first
            # self.valid_set = True
            # self.model.fit(np.array(train_x.values, dtype=float),
            #                np.array(train_df[self.target_column].values, dtype=float),
            #                eval_metric=xgb_mape,
            #                verbose=100,
            #                early_stopping_rounds=50,
            #                eval_set=[(np.array(valid_x.values, dtype=float), np.array(valid_df[self.target_column].values, dtype=float))])
            self.model.fit(np.array(feature_df, dtype=float),
                           target_array,
                           eval_set=self.valid_set,
                           eval_metric=xgb_mape,
                           verbose=False)
        else:
            self.model.fit(feature_df, target_array)

        # save the model
        if self.save_model:
            # TODO: save the best model if do parameter optimization
            # Need to verify
            if self.param_optimize:
                if self.logger:
                    self.logger.info('Train Model: {} model. best param: {}'.format(self.model_name, self.model.best_params_))
                    # self.logger.info('Train Model: {} model. best param: {}'.format(self.model_name, self.model_param))

                save_model_to_file(self.model.best_estimator_, self.model_path)
                # save_model_to_file(self.model, self.model_path)
            else:
                save_model_to_file(self.model, self.model_path)

        # predict using the train data, get the train error
        if self.model_name =='XGB':
                if self.model.get_booster()=='dart':
                    pred_array = self.model.predict(feature_df.values, ntree_limit=TrainModel.num_round)
                else:
                    pred_array = self.model.predict(feature_df.values)
        else:
            pred_array = self.model.predict(feature_df.values)
        train_result = pd.DataFrame({"true": target_array, "predict": pred_array})
        print("****"+str(len(train_result)))
        print("the train_result is:" + str(len(train_result)))
        error = mean_abs_percentage_error(train_result["true"].values, pred_array)
        if self.logger:
            self.logger.info("Use trained model, the training error is {}".format(error))
        data_dict.update({"model": self.model, "train_result": train_result})
        return data_dict

    @staticmethod
    def predict_by_model(model, data_df, feature_columns=None):
        if feature_columns is None:
            feature_array = data_df.values
        else:
            feature_array = data_df[feature_columns].values
        pred_array = model.predict(feature_array)
        return np.asarray(pred_array)

    @staticmethod
    def model_selection(cv_result):
        # a list of cross-validation results and choose the best one
        # currently use the default error function "mean_abs_percentage_error"
        # TODO: make it flexible to set different error function
        min_error = 10000
        min_index = -1
        for n, cur_cv in enumerate(cv_result):
            cur_result = cur_cv["result"]
            # error = mean_abs_percentage_error(cur_result["true"], cur_result["predict"])
            error = mean_abs_percentage_error(cur_result["true"].values, cur_result["predict"].values)  # modify by peng
            if error < min_error:
                min_error = error
                min_index = n
        return cv_result[min_index]["parameter"]

    @staticmethod
    def get_model_by_name(model_name):
        model_map = {
            'SVR': SVR(),
            'GBDT': GradientBoostingRegressor(),
            'GPR': GaussianProcessRegressor(),
            'LR-L1': Lasso(),
            "RF-reg": RandomForestRegressor(),
            'LR-L2': Ridge()
        }
        if xgb_installed:
            model_map['XGB'] = xgb.XGBRegressor()
        if lgt_installed:
            model_map['LGB'] = lgt.LGBMRegressor()

        if (model_name == 'XGB') and not xgb_installed:
            raise ModuleNotFoundError('the xgboost model is not installed!')
        return model_map[model_name]

    @staticmethod
    def get_model_default_param(model_name):
        default_param = {
            'SVR': {'kernel': 'rbf', 'gamma': 0.005, 'C': 5000, 'epsilon': 0.0015, 'tol': 0.0001, 'verbose': True},
            'XGB': {'n_estimators': 200, 'subsample': 0.90, 'colsample_bytree': 0.85, 'learning_rate': 0.05},
            'LGB': {'min_data_in_leaf': 20, 'feature_fraction': 0.90, 'colsample_bytree': 0.85, 'learning_rate': 0.05,
                    'objective':'quantile'},
            'GBDT': {'max_depth': 5, 'learning_rate': 0.05, 'n_estimators': 300},
            'LR-L1': {},
            'LR-L2': {},
            'RF-reg': {"n_estimators": 100},
            'GPR': {'kernel': DotProduct() + WhiteKernel()},
            'KNNR': {'n_neighbors': 5}
        }
        return default_param[model_name]

    @staticmethod
    def get_model_param_space(model_name):
        grid_search_params = {
            'SVR': {
                'gamma': [0.01, 0.1, 1],
                'C': [1, 10, 100, 500, 1000, 10000]
            },

            'XGB': {
                'max_depth': [3, 4],
                'n_estimators': [50, 100, 150]
            },

            'LGB': {
                "boosting_type": ['gbdt', 'dart', 'rf', 'goss'],
                'max_depth': [3, 4, 5],
                'learning_rate': [0.01, 0.05,  0.1],
                'n_estimators': [50, 100, 150]
            },

            'GBDT': {
                'max_depth': [3, 4, 5, 7, 10],
                'max_features': ['sqrt', 'log2', None],
                'loss': ['ls', 'huber'],
                'learning_rate': [0.01, 0.05,  0.1],
                'n_estimators': [500, 2000, 3000],
                'subsample': [0.5,  0.8,  1.0]
            },
            "LR-L1": {

            },
            "RF-reg": {
                'max_depth': [5, 7],
                "n_estimators": [500, 800]
            }
        }
        return grid_search_params[model_name]

    @staticmethod
    def build_parameter_combination(model_name):
        param_space = TrainModel.get_model_param_space(model_name)
        keys = param_space.keys()
        param_list = [param_space[key] for key in keys]
        all_value_combination = list(itertools.product(*param_list))
        all_param_list = []
        for value_list in all_value_combination:
            all_param_list.append(dict(zip(keys, value_list)))
        return all_param_list

    @staticmethod
    def split_train_validate(time_series, forecast_days, minimum_train_ratio=2):
        # assuming the data is sorted
        year_month_list = np.column_stack([time_series.dt.year, time_series.dt.month])
        year_month_tuple = np.unique(year_month_list, axis=0)
        minimum_month = int(np.ceil(forecast_days * (1 + minimum_train_ratio) / 30))
#        minimum_month = int(forecast_days * (1 + minimum_train_ratio) / 30)
        total_month = len(year_month_tuple)

        index_list = []
        for n in range(minimum_month, total_month + 1):
            valid_year, valid_month = year_month_tuple[n - 1][0], year_month_tuple[n - 1][1]
            valid_index = np.where((year_month_list[:, 0] == valid_year) & (year_month_list[:, 1] == valid_month))[0]
            if minimum_month == n:
                train_index = list(range(valid_index[-1]+1, len(year_month_list)))
            else:
                train_index = list(range(0, valid_index[0]))
            index_list.append({"train": train_index, "valid": valid_index})
        return index_list




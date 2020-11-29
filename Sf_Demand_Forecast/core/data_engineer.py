#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
# @Time    : 2019/9/30 11:29
# @Author  : peng.wang
# @Email   : WangPeng4@sfmail.sf-express.com
# @FileName: data_engineer.py
# @ProjectName :sh-demand-forecast-alg
"""
import os
import pandas as pd
import numpy as np
from sklearn.base import TransformerMixin
from scipy.special import boxcox, inv_boxcox
from sklearn.preprocessing import MaxAbsScaler, MinMaxScaler, StandardScaler, RobustScaler
#
from utils.misc import Logger

log = Logger(log_path=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'log')).logger


# TODO: to implement the Data Clean class
class DataClean(TransformerMixin):
    def __init__(self, fill_value=None, time_series=False, time_column='date_time',
                 target_column='quantity', search_range=5,
                 logger=log):
        self.df = None
        self.fill_value = fill_value
        self.target_column = target_column
        self.time_column = time_column
        self.time_series = time_series
        self.search_range = search_range
        self.logger = logger

    def fit(self):
        return self

    def transform(self, df):
        self.logger.info('...DataFormat: calc data clean...')
        self.df = df

        # TODO: may not need this part any more
        if self.time_series:
            try:
                df[self.time_column] = pd.to_datetime(df[self.time_column], errors='coerce')
            except ValueError:
                if self.logger:
                    self.logger.error('DataFormat: time column error!')
        self.handle_missing_target()
        return self.df

    def handle_missing_target(self):
        # handle the na for target column
        invalid_index = self.df[(self.df[self.target_column].isnull()) | (self.df[self.target_column].isnull())].index
        if len(invalid_index) > 0:
            if self.logger:
                self.logger.warning("Index missing before filling...")
                self.check_missing_data()
        else:
            if self.logger:
                self.logger.info("No index missing columns.")

        invalid_index_set = set(invalid_index)
        for index in invalid_index:
            search_index = set(range(index - self.search_range, index + self.search_range + 1))
            valid_search_index = search_index - invalid_index_set
            if len(valid_search_index) > 1:
                if self.fill_value:
                    self.df.fillna(self.fill_value, inplace=True)
                else:
                    fill_mean = np.mean(self.df.loc[valid_search_index, self.target_column])
                    self.df.loc[index, self.target_column] = fill_mean
        if self.logger:
            self.logger.info("Index missing after the filling...")
            self.check_missing_data()

        # fill the remaining na by 0
        self.df.fillna(0, inplace=True)

    def check_missing_data(self):
        missing_index = self.df[self.df[self.target_column].isnull()].index
        missing_percentage = 1.0 * len(missing_index) / len(self.df)
        if self.logger:
            self.logger.info("Total missing values percentage is {}".format(missing_percentage))
            date_str_series = self.df.loc[missing_index, self.time_column].apply(lambda time_value: time_value.strftime("%Y-%m-%d"))
            missing_info = np.unique(date_str_series, return_counts=True)
            self.logger.info(dict(zip(missing_info[0], missing_info[1])))


# Data merge class
class DataMerge(TransformerMixin):
    def __init__(self, df_b, time_column='date_time', how_method='left', logger=None):
        self.df_a = None
        self.df_b = df_b
        self.time_column = time_column
        self.merge_key = time_column
        self.how_method = how_method
        self.logger = logger

    def fit(self):
        return self

    def transform(self, df_a):
        # TODO: in future it needs to resample data to do the merge
        if self.logger:
            self.logger.info('...DataFormat: calc datamerge...')
        self.df_a = df_a
        if self.merge_key:
            # self.df_a["date"] = self.df_a[self.time_column].dt.date
            df_b = self.df_b.copy(deep=True)
            # df_b["date"] = df_b[self.time_column].dt.date
            # df_b.drop(self.time_column, axis=1, inplace=True)
            return pd.merge(self.df_a, df_b, how=self.how_method, on=self.merge_key)

        if self.merge_key is None:
            if self.logger:
                self.logger.error('DataFormat: data merge key missing!')
            return None
        else:
            return pd.merge(self.df_a, self.df_b, how=self.how_method, on=self.merge_key)


# self-define Scaler
class BoxcoxScaler(object):

    def fit_transform(self, y):
        return boxcox(y, 0.5)

    def inverse_transform(self, y):
        return inv_boxcox(y, 0.5)


# self-define Scaler
class LogScaler:

    def fit_transform(self, y):
        y = np.log1p(y)
        return y

    def inverse_transform(self, y):
        y = np.expm1(y)
        y[y == 1] = 0
        return y

    def transform(self, y):
        y = np.log1p(y)
        return y


# data scaler class
class DataScaler(TransformerMixin):
    """
    Doing data scaling using selected scaler based on the scaler name
    :param  scaler_name:
    :param  target_column
    """

    def __init__(self, scaler_name, target_column='quantity'):
        """

        :param scaler_name:
        """
        self.scaler_name = scaler_name
        self.target_column = target_column
        self.scale = None

    def fit(self):
        return self

    def transform(self, df):
        target = df[self.target_column]
        self.scale = self.get_scaler()
        target = self.scale.fit_transform(target).ravel()
        df[self.target_column] = target
        return df

    def get_scaler(self):

        scaler_map = {'MaxAbs': MaxAbsScaler(),
                      'MinMax': MinMaxScaler(),
                      'Standard': StandardScaler(),
                      'Robust': RobustScaler(),
                      'Boxcox': BoxcoxScaler(),
                      'Log': LogScaler()}
        return scaler_map[self.scaler_name]

# TODO: define your own data operation class using the same format, in order to use Pipeline


if __name__ == "__main__":
    import yaml
    from sklearn.pipeline import Pipeline
    from core.data_handle import DataHandler

    with open(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config/config.yaml')) as fp:
        config = yaml.load(fp)

    data_ins = DataHandler(data_path=config['DATA']['data_path'], time_column=config['DATA']['time_column'],
                           id_column=config['DATA']['id_column'], target_column=config['DATA']['target_column'],
                           multiple_sku=True)

    pipelist = []
    dataclear = DataClean(time_series=True, time_column=config['DATA']['time_column'],
                          target_column=config['DATA']['target_column'])
    pipelist.append(('dataclean', dataclear))

    datascaler = DataScaler(scaler_name=config['DATA']['scaler_name'],target_column=config['DATA']['target_column'])
    pipelist.append(('datascaler', datascaler))
    DataPipe = Pipeline(pipelist)
    df = DataPipe.transform(data_ins.df)
    print(df)


#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
# @Time    : 2019/10/29 9:38
# @Author  : peng.wang
# @Email   : WangPeng4@sfmail.sf-express.com
# @FileName: data_handle.py
# @ProjectName :sh-demand-forecast-alg
"""
import os
import yaml
import warnings
import pandas as pd
import numpy as np
import scipy
#
from utils.misc import Logger
warnings.filterwarnings("ignore")
log = Logger(log_path=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'log')).logger
# log = Logger(log_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'log')).logger


class DataHandler(object):
    """
    provide multiple interface for loading data from local or HDFS platform
    processing data after being loaded, then generate standard format output data
    structure
    """
    def __init__(self, data_path, time_column='date', id_column='id', target_column='quantity', data_column=None,
                 data_mode='time series', data_interval=1, with_header=False, data_from_hdfs=False,
                 multiple_sku=False, start_date_str=None, end_date_str=None, logger=log):
        """

        :param data_paths: path of data being loaded. if data_from_hdfs = False, then provide the local path,else provide
        the path of hdfs
        :param data_mode: treat the data as time series data or normal data."time series" or "normal"
        :param data_interval: if data_mode='time series', then this parameter must be provide. the unit is day.
        data_interval = 1, then means one day
        :param with_header: the csv file with header or not
        :param with_external: indicator for using external feature data
        :param data_from_hdfs: the data source come from hdfs or local
        :param multiple_sku: the data contain multiple sku or just single sku
        :param start_date_str: the start date string
        :param end_date_str: the end date string
        :return:
            the dataframe of data: df
            the resample dataframe: resample_df
            the groupby_id dict； df_by_id
        """
        self.data_path = data_path
        self.data_columns = data_column
        self.datetime_column = time_column
        self.target_column = target_column
        # self.date_format = date_format
        self.id_column = id_column
        self.data_mode = data_mode
        self.data_interval = data_interval
        self.with_header = with_header
        # self.with_external = with_external
        # self.external_df = external_df
        self.data_from_hdfs = data_from_hdfs
        self.multiple_sku = multiple_sku
        self.start_date_str = start_date_str
        self.end_date_str = end_date_str
        self.logger = logger
        self.df = None

        if not self.data_from_hdfs:
            self.read_data_from_local()
            self.postprocessing()
        else:
            self.df = self.read_data_from_hdfs()
            self.postprocessing()

        if self.multiple_sku:
            self.group_data_by_id()
        # if self.with_external:
        #     self.external_df = external_df

    # read data from local
    def read_data_from_local(self):
        if self.with_header:
            df = pd.read_csv(self.data_path, names=self.data_columns, skiprows=1)
        else:
            df = pd.read_csv(self.data_path, names=self.data_columns)
        # transform the datetime column to Datetime type
        df[self.datetime_column] = df[self.datetime_column].astype(str)
        df[self.datetime_column] = pd.to_datetime(df[self.datetime_column])

        if self.logger:
            self.logger.info("The training data is \n")
            self.logger.info(df.head())
        self.df = df

    # def read_data_from_external(self):
    #     """
    #     read data from external file
    #     :return:
    #     """
    #     if self.with_header:
    #         external_df = pd.read_csv(self.external_path, names=self.data_columns, skiprows=1)
    #     else:
    #         external_df = pd.read_csv(self.external_path, names=self.data_columns)
    #     # transform the datetime column to Datetime type
    #     external_df[self.datetime_column] = external_df[self.datetime_column].astype(str)
    #     external_df[self.datetime_column] = pd.to_datetime(external_df[self.datetime_column])
    #
    #     if self.logger:
    #         self.logger.info("The external training data is \n")
    #         self.logger.info(external_df.head())
    #     self.external_df = external_df

    def read_data_from_hdfs(self):
        from pyspark.sql import SparkSession
        spark = SparkSession \
            .builder \
            .appName("Building models using storage data") \
            .enableHiveSupport() \
            .getOrCreate()

        data = spark.read.parquet(self.data_path)
        data_unique = data.dropDuplicates()

        for col in ["time", "point"]:
            if col in data_unique.columns:
                data_unique = data_unique.drop(col)

        # filter wrong data
        data_df = data_unique.filter(~data_unique.value.startswith('{"value"')).toPandas()
        data_df.rename(columns={"local_time": self.datetime_column, "value": self.target_column}, inplace=True)
        # print(data_df.head())
        spark.stop()
        return data_df

    # resample data based on the data_interval, when doing upsampling, fill the data with 0 in default
    def resample_data(self):
        self.df.index = self.df[self.datetime_column]
        resample_df = self.df.resample('{}D'.format(self.data_interval)).sum()
        resample_df.reset_index(inplace=True)
        resample_df.sort_values(by=self.datetime_column, inplace=True)
        self.resample_df = resample_df

    def extract_data_by_date(self):
        start_date = pd.to_datetime(self.start_date_str)
        end_date = pd.to_datetime(self.end_date_str)
        self.resample_df = self.resample_df[(self.resample_df[self.datetime_column] >= start_date) &
                                            (self.resample_df[self.datetime_column] <= end_date)]
        self.resample_df.reset_index(drop=True, inplace=True)

    def filter_target_values(self, smoothing=False, window_size=7):
        # The following steps are applied:
        # 1. filter invalid values, like Nan or inf
        if self.logger is not None:
            self.logger.info("Before conduct data cleaning, the number of rows is {}".format(self.df.shape[0]))

        self.df[self.target_column] = self.df[self.target_column].astype(float)
        null_index = self.df[(self.df[self.target_column].isnull()) |
                             (self.df[self.target_column] == np.inf)].index
        # drop all invalid data based on the target value
        self.df.drop(null_index, axis=0, inplace=True)

        # using median filter denoise
        if smoothing:
            self.df[self.target_column] = scipy.signal.medfilt(self.df[self.target_column].values, window_size)
        self.df.sort_values(by=self.datetime_column, inplace=True)
        if self.logger is not None:
            self.logger.info("After conduct data cleaning, the number of rows is {}".format(self.df.shape[0]))

    # post processing of data
    def postprocessing(self):
        self.filter_target_values()
        self.resample_data()
        if self.start_date_str is not None and self.end_date_str is not None:
            self.extract_data_by_date()

    # group data by gcode
    def group_data_by_id(self):
        data_dict = dict()
        for index, group in self.df.groupby(self.id_column):
            group.reset_index(drop=True, inplace=True)
            data_dict[str(index)] = group
        self.df_by_id = data_dict


if __name__ == '__main__':
    from tsfresh import extract_features
    from tsfresh import select_features
    from tsfresh.utilities.dataframe_functions import impute
    from utils.util import generate_cutoffs

    with open(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config/config.yaml')) as fp:
        config = yaml.load(fp)
    data_ins = DataHandler(data_path='../data/data.csv', time_column=config['DATA']['time_column'],
                           id_column=config['DATA']['id_column'], target_column='quantity',#target_column=config['DATA']['target_column'],
                           multiple_sku=True)#, #with_external=False, external_path=config['DATA']['data_path'])

    # timeseries= data_ins.df[[config['DATA']['time_column']]+[config['DATA']['id_column']]+[config['DATA']['target_column']]]
    # y=data_ins.df[config['DATA']['target_column']]
    # extracted_features = extract_features(timeseries, column_id=config['DATA']['id_column'],
    #                                                      column_sort=config['DATA']['time_column'])
    # impute(extracted_features)
    # extracted_features = extracted_features.loc[:, extracted_features.apply(pd.Series.nunique) != 1]  # 粗略筛一遍
    # print('tsfresh:\n')
    # print(extracted_features.to_csv('ts_fresh.csv'))
    data_dict = data_ins.df_by_id
    # print(data_ins.external_df)
    from monthdelta import monthdelta
    df = data_dict['1197']
    print(df.head())
    print(df.tail())
    df = df[df['date'] >= '2018-01-01']
    print(df.head())
    df.reset_index(inplace=True, drop=True)
    print(df.head())
    # horizon = monthdelta(1)
    # period = monthdelta(1)
    # train_end_date ='2019-03-30'
    # # test_end_date = '2019-07-31'
    # cutoffs = generate_cutoffs(df, config['DATA']['time_column'], horizon, period=period, train_end_date=train_end_date,
    #                            test_end_date='2019-09-01')
    # for cutoff in cutoffs:
    #     print(cutoff)





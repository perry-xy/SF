#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
# @Time    : 2019/10/8 22:34
# @Author  : peng.wang
# @Email   : WangPeng4@sfmail.sf-express.com
# @FileName: util.py
# @ProjectName :sh-demand-forecast-alg
"""
import pandas as pd
import numpy as np
import requests
import json
import os
from utils.misc import Logger
log = Logger(log_path='log').logger


class DataLoader(object):
    """
    Data loader. Combines a dataset and a sampler, and provides an iterable over
    the given dataset.
    """
    def __init__(self, data, train_len, pred_len, feature_names, target_name, append_train=False):
        self.data = data
        self.train_len = train_len
        self.pred_len = pred_len
        self.feature_names = feature_names
        self.target_name = target_name
        self.append_train = append_train

    def __iter__(self):
        return self

    def __next__(self):
        """

        :return:
        """
        if not isinstance(self.data, pd.DataFrame):
            raise ValueError('the data input should be DataFrame')
        if self.append_train:
            pass
        else:
            pass


def fourier_series(date_series, period, series_order):
    t = np.array(
        (date_series - pd.datetime(1970, 1, 1))
            .dt.total_seconds()
            .astype(np.float)
    ) / (3600 * 24.)

    value = np.column_stack(
        [fun((2.0 * (i + 1) * np.pi * t / period)) for i in range(series_order) for fun in (np.sin, np.cos)])
    return value


def generate_cutoffs(df, time_column, horizon, period, train_end_date, test_end_date=None):
    """Generate cutoff dates
    Parameters
    ----------
    df: pd.DataFrame with historical data.
    horizon: pd.Timedelta forecast horizon. #预测时间范围
    train_end_date:
    period: pd.Timedelta simulated forecasts are done with this period.
    test_end_date:
    Returns
    -------
    list of pd.Timestamp
    """
    # Last cutoff is 'latest date in data - horizon' date

    train_end_date = pd.to_datetime(train_end_date) #2019.06.01
    if test_end_date is None:
        cutoff = df[time_column].max()-horizon #最后一个切割时间：最大时间-7天
    else:
        test_end_date = pd.to_datetime(test_end_date)
        if test_end_date > df[time_column].max():
            raise ValueError('test_end_date is exceed the max time.')
        cutoff = test_end_date - horizon
    if cutoff < df[time_column].min():
        raise ValueError('Less data than horizon.')
    result = [cutoff]
    while result[-1] >= train_end_date:
        cutoff -= period
        # If data does not exist in data range (cutoff, cutoff + horizon]
        if not (((df[time_column] > cutoff) & (df[time_column] <= cutoff + horizon)).any()):
            # Next cutoff point is 'last date before cutoff in data - horizon'
            if cutoff > df[time_column].min():
                closest_date = df[df[time_column] <= cutoff].max()[time_column]
                cutoff = closest_date - horizon
            # else no data left, leave cutoff as is, it will be dropped.
        result.append(cutoff)
    result = result[:-1]
    if len(result) == 0:
        raise ValueError(
            'Less data than horizon after initial window. '
            'Make horizon or initial shorter.'
        )
    log.info('Making {} forecasts with cutoffs between {} and {}'.format(
        len(result), result[-1], result[0]
    ))
    print("the result is: \n")
    print(result)
    return reversed(result)


class GetExternalData(object):
    """
    get the external data with public APIs
    including holiday ,weather information
    """
    holiday_base_url = 'http://api.goseek.cn/Tools/holiday'
    weather_base_url = ''

    def __init__(self, start_date, end_date, interval='D', time_column='date',  with_holiday=True, with_weather=False, logger=log):
        self.start_date = start_date
        self.end_date = end_date
        self.interval = interval
        self.with_holiday = with_holiday
        self.with_weather = with_weather
        self.date_range = pd.date_range(self.start_date, self.end_date, freq=self.interval)
        self.df = pd.DataFrame(self.date_range, columns=[time_column])
        self.feature_list = []
        self.time_column = time_column
        self.log = logger

    def get_holiday(self, file_save=True):
        holiday_dict = {}
        holiday = []
        for date in self.date_range:
            date = date.strftime('%Y%m%d')
            url = GetExternalData.holiday_base_url +'?date={}'.format(date)
            result = requests.get(url)
            content = json.loads(result.content)
            if content['code']==10000:
                holiday.append(content['data'])
        holiday_dict[self.time_column] = self.date_range
        holiday_dict['holiday'] = holiday
        df_holiday = pd.DataFrame(holiday_dict)
        if file_save:
            df_holiday.to_csv('holiday.csv')
        return df_holiday

    def get_weather(self, file_save=True):
        df_weather = pd.DataFrame()

        return df_weather

    def get_external(self):
        """
        get the external dataframe via public apis
        :return:
        """
        if self.with_holiday:
            self.log.info('pulling holiday information from public api....')
            if os.path.exists('holiday.csv'):
                self.log.info('read holiday data direct from local file "holiday.csv" ')
                df_holiday = pd.read_csv('holiday.csv', usecols=[self.time_column, 'holiday'])
                df_holiday[self.time_column] = pd.to_datetime(df_holiday[self.time_column])

            else:
                df_holiday = self.get_holiday()
                self.log.info('pulling holiday information from public api is over')
            self.log.info('df_holiday:')
            self.log.info(df_holiday.head())
            self.update_external_df(df_holiday)

        if self.with_weather:
            df_weather = self.get_weather()
            self.update_external_df(df_weather)
        return self.df

    def update_external_df(self, feature_df):
        self.df = pd.merge(self.df, feature_df, on=self.time_column)
        self.feature_list.extend(feature_df.columns.tolist())


if __name__ == '__main__':
    ExternalData = GetExternalData(start_date='2017-01-01', end_date='2019-12-31')
    df_external = ExternalData.get_external()
    print(df_external)





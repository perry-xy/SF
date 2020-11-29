#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
# @Time    : 2019/9/30 11:29
# @Author  : peng.wang
# @Email   : WangPeng4@sfmail.sf-express.com
# @FileName: feature_engineer.py
# @ProjectName :sh-demand-forecast-alg
"""

import warnings
import math
import os
import pandas as pd
import numpy as np
from datetime import date, timedelta
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.feature_selection import VarianceThreshold, SelectFromModel
from sklearn.feature_selection import SelectKBest, f_regression
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LassoCV
from tsfresh import extract_features
from tsfresh.utilities.dataframe_functions import impute
#
from utils.util import fourier_series
from utils.misc import Logger
from config.config import FORECAST_1_MONTH_VALUE, FORECAST_1_WEEK_VALUE, EXTERNAL_COLUMNS
warnings.filterwarnings("ignore")
log = Logger(log_path=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'log')).logger


# Custom Transformer that extracts columns passed as argument to its constructor
class FeatureEngineer(BaseEstimator, TransformerMixin):
    """
    FeatureEngineer generate the following features:
    1. lag features
    2. time features
    3. group features
    4. external features(weather feature and holiday feature) not in default
    5. tsfresh features not in default
    """
    # Class Constructor
    def __init__(self, forecast_period=None, data_interval=None, time_column='date_time',
                 id_column='id', target_column='target', with_external=False, external_column=EXTERNAL_COLUMNS,
                 external_df=None, time_features=True, group_features=True,
                 lag_features=True, tsfresh_features=False, logger=log):

        self.time_features = time_features
        self.group_features = group_features
        self.lag_features = lag_features
        self.tsfresh_features = tsfresh_features
        self.feature_list = []
        self.df_out = pd.DataFrame()
        self.data_interval = data_interval
        self.shift_day_list = range(1, 5)
        self.lag_point_list = []
        self.forecast_period = forecast_period
        self.time_column = time_column
        self.target_column = target_column
        self.id_column = id_column
        self.key_columns = [id_column] + [time_column]
        self.with_external = with_external
        self.external_columns = external_column + [time_column]
        self.external_df = external_df
        self.logger = logger

        # Return self nothing else to do here
    def fit(self):
        return self

    # Method that describes what we need this transformer to do
    def transform(self, df):

        if self.logger:
            self.logger.info("...FeatureEngineering...")
        if self.data_interval < 4*60:
            self.daily = True
        #
        # self.df_out = pd.concat([self.df_out, df[self.key_columns+[self.target_column]]], axis=1)
        self.df_out = pd.concat([self.df_out, df], axis=1)
        # target based features
        if self.lag_features:
            lag_point_list = self.set_lagging_period(self.shift_day_list, self.forecast_period, self.data_interval)
            self.lag_point_list = lag_point_list
            shift_feature_df = FeatureEngineer.calc_shift_feature(self.df_out[[self.target_column]], self.lag_point_list)
            print(shift_feature_df[:10])
            self.update_features(shift_feature_df)
            # rolling_feature_df = FeatureEngineer.calc_rolling_feature(self.df_out[[self.target_column]],
            #                                                           self.lag_point_list, windows=15)
            # self.update_features(rolling_feature_df)
            #
            # ewm_feature_df = FeatureEngineer.calc_ewm_feature(self.df_out[[self.target_column]],
            #                                                   self.lag_point_list, alpha=0.8)
            # self.update_features(ewm_feature_df)
        # time features
        if self.time_features:
            time_features_df = FeatureEngineer.calc_time_feature(df[self.time_column])
            self.update_features(time_features_df)
        # group features
        if self.group_features:
            group_column_list = ["weekday", "month"]
            group_feature_df = FeatureEngineer.calc_group_feature(self.df_out[group_column_list + [self.target_column]],
                                                                  group_column_list, self.target_column,
                                                                  self.lag_point_list)
            self.update_features(group_feature_df)

            # compre_column_list = [self.target_column]
            # compre_feature_df = FeatureEngineer.calc_comprehensive_feature(self.df_out, compre_column_list)
            # self.update_features(compre_feature_df)
        # external features: weather features and holiday feature
        if 'holiday' in self.df_out.columns:
            holiday_feature_df = calc_holiday_feature(self.df_out)
            self.update_features(holiday_feature_df)
        if 'temperature' in self.df_out.columns:
            weather_feature_df = calc_weather_feature(self.df_out[['temperature', 'humidity']])
            self.update_features(weather_feature_df)
            self.update_features(self.external_df)
            self.df_out.drop_duplicate()
        # tsfresh features
        if self.tsfresh_features:
            tsfresh_column_list = []
            tsfresh_feature_df = FeatureEngineer.calc_tsfresh_feature(df, self.id_column, self.target_column)
            self.update_features(tsfresh_feature_df)
        # TODO: add your own customized feature function to generate features

        # drop the duplicate columns
        self.df_out = self.df_out.loc[:, ~self.df_out.columns.duplicated()]
        print(self.df_out['quantity_shift_7'][:10])
        return self.df_out

    @staticmethod
    def set_lagging_period(shift_day_list, forecast_period, data_interval):
        """
        :param shift_day_list:
        :param forecast_period:
        :param data_interval: in the unit of day
        :return:
        """
        daily_points = int(data_interval)
        if forecast_period == FORECAST_1_MONTH_VALUE:
            forecast_days = 30
        elif forecast_period == FORECAST_1_WEEK_VALUE:
            forecast_days = 7
        else:
            forecast_days = 1
        shift_center_list = [(value-1 + forecast_days) * daily_points for value in shift_day_list]
        shift_list = []
        for center in shift_center_list:
            shift_list.append(center)
        return shift_list

    @staticmethod
    def get_timespan(df, dt, minus, periods, freq='D'):
        return df[pd.date_range(dt - timedelta(days=minus), periods=periods, freq=freq)]

    @staticmethod
    def group_feature(df, group_column, target_column, lag_point_list):
        """
        calculate the grouped values by removing invalid data, e.g. zero or nan values.
        when df is the training data, it may contain nan values.
        when df is the combination of historical data and testing data, it may contain nan values and zero values.
        The latter one is from testing data.
        :param df:
        :param group_column:
        :param target_column:
        :return:
        """
        # remove the target_column is zero, this is the case when calculating the features for testing data
        zero_index = df[df[target_column] <= 0].index

        def remove_invalid_values(value_arr):
            # remove nan and 0
            return value_arr[(~np.isnan(value_arr)) & (value_arr > 0)]

        group_features = np.zeros((len(df), 5))
        for id, groups in df[~df.index.isin(zero_index)].groupby(group_column):
            value_arr = groups[target_column].values
            value_arr = remove_invalid_values(value_arr)
            stats_vec = [np.max(value_arr), np.min(value_arr), np.median(value_arr), np.std(value_arr), np.sum(value_arr)]
            assign_index = df[df[group_column] == id].index
            group_features[assign_index, :] = stats_vec

        group_columns = ["{}_groupby_{}_{}".format(target_column, group_column, i) for i in
                         ["max", "min", "median", "std", "sum"]]
        return pd.DataFrame(group_features, columns=group_columns)

    def update_features(self, feature_df):
        self.df_out = pd.concat([self.df_out, feature_df], axis=1)
        self.feature_list.extend(feature_df.columns.tolist())
        print(self.df_out['quantity_shift_7'][:10])
        print(self.feature_list)

    @staticmethod
    def calc_shift_feature(target_series, lag_point_list):
        # TODO: make it flexible to return the features in different periods
        df_list = []
        shift_columns = []
        for i, shift_num in enumerate(lag_point_list):
            col_name = '{}_shift_{}'.format(target_series.columns[0], shift_num)
            shift_columns.append(col_name)
            cur_series = target_series.shift(shift_num)
            cur_series.rename({target_series.columns[0]: col_name}, axis=1, inplace=True)
            df_list.append(cur_series)
        return pd.concat(df_list, axis=1)

    @staticmethod
    def calc_rolling_feature(target_series, lag_point_list, windows, min_periods=10, win_type='triang'):
        df_list = []
        shift_columns = []
        for i, shift_num in enumerate(lag_point_list):
            col_name = '{}_shift_{}_rmean'.format(target_series.columns[0], shift_num)
            shift_columns.append(col_name)
            cur_series = target_series.shift(shift_num).rolling(window=windows, min_periods=min_periods,
                                                                win_type=win_type).mean()
            cur_series.rename({target_series.columns[0]: col_name}, axis=1, inplace=True)
            df_list.append(cur_series)
        return pd.concat(df_list, axis=1)

    @staticmethod
    def calc_ewm_feature(target_series, lag_point_list, alpha, min_periods=10):
        df_list = []
        shift_columns = []
        for i, shift_num in enumerate(lag_point_list):
            col_name = '{}_ewm_{}_rmean'.format(target_series.columns[0], shift_num)
            shift_columns.append(col_name)
            cur_series = target_series.shift(shift_num).ewm(alpha=alpha,
                                                            min_periods=min_periods).mean()
            cur_series.rename({target_series.columns[0]: col_name}, axis=1, inplace=True)
            df_list.append(cur_series)
        return pd.concat(df_list, axis=1)

    @staticmethod
    def get_timespan(df, dt, minus, periods, k, freq='D'):
        return df.loc[pd.date_range(dt - timedelta(days=minus + k), periods=periods, freq=freq)]

    @staticmethod
    def calc_comprehensive_feature(df, target_columns, future_day=30, past_week=[7, 14, 30, 60, 90]):
        # 过去i周-（加权）平均值，差分均值，中值，最小，最大值，标准差
        X = {}
        for target_column in target_columns:
            X[target_column] = {}
            for j in date:
                X[target_column][j] = {}
                for i in past_week:
                    for k in range(1, 1 + future_day):
                        tmp = FeatureEngineer.get_timespan(df[target_column], j, i, i, k - 1)
                        X[target_column][j][target_column + '_diff_%s_mean' % i + '_future' + str(k) + 'day'] = tmp.diff().mean()
                        X[target_column][j][target_column + '_mean_%s_decay' % i + '_future' + str(k) + 'day'] = (
                                    tmp * np.power(0.9, np.arange(i)[::-1])).sum()
                        X[target_column][j][target_column + '_mean_%s' % i + '_future' + str(k) + 'day'] = tmp.mean()
                        X[target_column][j][target_column + '_median_%s' % i + '_future' + str(k) + 'day'] = tmp.median()
                        X[target_column][j][target_column + '_min_%s' % i + '_future' + str(k) + 'day'] = tmp.min()
                        X[target_column][j][target_column + '_max_%s' % i + '_future' + str(k) + 'day'] = tmp.max()
                        X[target_column][j][target_column + '_std_%s' % i + '_future' + str(k) + 'day'] = tmp.std()
            #     df_time_series=pd.DataFrame(columns=list())
            df_time = pd.DataFrame(X[target_column]).T
        return df_time

    @staticmethod
    def calc_group_feature(dft, group_column_list, target_column, lag_point_list):
        # TODO: make it flexible to return the features in different periods
        df_list = []
        for group_column in group_column_list:
            cur_features = FeatureEngineer.group_feature(dft[[group_column, target_column]], group_column,
                                                         target_column, lag_point_list)
            df_list.append(cur_features)

        return pd.concat(df_list, axis=1)

    @staticmethod
    def calc_tsfresh_feature(df, id_column, target_column, time_column):
        reduced_df = df[[time_column] + [id_column] + [target_column]]
        extracted_features = extract_features(reduced_df, column_id=id_column,
                                              column_sort=time_column)
        impute(extracted_features)
        extracted_features = extracted_features.loc[:, extracted_features.apply(pd.Series.nunique) != 1]  # 粗略筛一遍
        extracted_features = extracted_features.reset_index()
        extracted_features.rename({'id': id_column}, axis=1, inplace=True)
        ts_feature = reduced_df.merge(extracted_features, on=id_column, how='left')
        ts_feature_df = ts_feature.drop(target_column)
        return ts_feature_df

    @staticmethod
    # time features
    def calc_time_feature(time_series, daily_feature=False, weekly_feature=True, yearly_feature=False):
        df_list = []
        # time feature from DatetimeIndex
        date_normal_features = np.column_stack((time_series.dt.year,
                                                time_series.dt.month,
                                                time_series.dt.day,
                                                time_series.dt.weekofyear,
                                                time_series.dt.weekday,
                                                time_series.dt.dayofyear,
                                                time_series.dt.quarter,
                                                time_series.dt.weekday.isin([5, 6]),
                                                time_series.dt.is_month_start,
                                                time_series.dt.is_month_end,
                                                time_series.dt.is_quarter_start,
                                                time_series.dt.is_quarter_end))
        date_normal_columns = ['year', 'month', 'day', 'weekofyear', 'weekday',
                               'dayofyear', 'quarter', 'is_weekend', 'is_month_start', 'is_month_end',
                               'is_quarter_start', 'is_quarter_end']
        df_date_normal_features = pd.DataFrame(date_normal_features, columns=date_normal_columns)
        df_list.append(df_date_normal_features)
        # date distance feature
        date_dis_columns = ["date_distance"]
        date_dis_features = (time_series - pd.datetime(1970, 1, 1, 0, 0, 0)).dt.days
        df_date_distance_features = pd.DataFrame(np.array(date_dis_features), columns=date_dis_columns)
        df_list.append(df_date_distance_features)
        # fourier feature with daily, weekly and yearly
        if daily_feature:
            daily_features = fourier_series(time_series, 1, 3)
            daily_columns = ['daily_delim_{}'.format(i + 1) for i in range(daily_features.shape[1])]
            df_daily_features = pd.DataFrame(daily_features, columns=daily_columns)
            df_daily_features.name = "time_daily_features"
            df_list.append(df_daily_features)

        if weekly_feature:
            weekly_features = fourier_series(time_series, 7, 5)
            weekly_columns = ['weekly_delim_{}'.format(i + 1) for i in range(weekly_features.shape[1])]
            df_weekly_features = pd.DataFrame(weekly_features, columns=weekly_columns)
            df_weekly_features.name = "time_weekly_features"
            df_list.append(df_weekly_features)

        if yearly_feature:
            yearly_features = fourier_series(time_series, 365.245, 1)
            yearly_columns = ['yearly_delim_{}'.format(i + 1) for i in range(yearly_features.shape[1])]
            df_yearly_features = pd.DataFrame(yearly_features, columns=yearly_columns)
            df_yearly_features.name = "time_yearly_features"
            df_list.append(df_yearly_features)
        return pd.concat(df_list, axis=1)


def calc_holiday_feature(dfh):
    # TODO: revise the holiday reference file by adding those special non-holidays
    return dfh[["holiday"]]


def calc_weather_feature(dfw):
    normal_features = np.column_stack((dfw.temperature,
                                       dfw.humidity,
                                       dfw.temperature / dfw.humidity * 100 / 40,
                                       dfw.temperature / 40 * dfw.humidity / 100,
                                       ((dfw.temperature > 14) & (dfw.temperature < 20)).astype(int),
                                       np.power((dfw.temperature - 21) / 9, 2) + np.power((dfw.humidity - 72) / 30, 2),
                                       (1.818 * dfw.temperature + 18.18) * (0.88 + 0.002 * dfw.humidity) + (
                                                   dfw.temperature - 32) / (45 - dfw.temperature) + 18.2))
    weather_columns = ["temperature", "humidity", "temperature_divide_humidity", "temperature2",
                       "temperature_sensitive", "temperature_oval", 'ssd_comfortableindex']
    df_weather_features = pd.DataFrame(normal_features, columns=weather_columns)
    return df_weather_features


class FeaturePostprocessing(object):
    def __init__(self, input_feature_type="train", feature_df=pd.DataFrame(), target_column="", time_column="",
                 key_columns=[], logger=None):
        self.input_feature_type = input_feature_type
        self.feature_df = feature_df
        self.target_column = target_column
        self.time_column = time_column
        self.key_columns = key_columns
        self.feature_list = []
        self.logger = logger

    def fit(self):
        return self

    def transform(self, feature_df):
        if self.logger:
            self.logger.info("...FeaturePostprocessing...")
        self.feature_df = feature_df
        all_columns = self.feature_df.columns
        exclude_column = self.key_columns + [self.target_column]
        self.feature_list = [col for col in all_columns if col not in exclude_column]

        self.handle_na_features()
        # TODO: add more post-processing functions
        # remove the features that has unique value which will provide few information when explain the target column
        # self.postprocessing_features()

        target_array = self.feature_df[self.target_column].values
        feature_df = self.feature_df.drop(exclude_column, axis=1)
        return {"target": target_array, "feature": feature_df, "time": self.feature_df[self.time_column]}

    def handle_na_features(self):
        # for training, drop the features
        total_samples = self.feature_df.shape[0]
        if self.logger:
            self.logger.info("......handle na features......")
            self.logger.info("before drop na features, training data size is {}".format(total_samples))

        # find any na feature values
        index_labels = self.feature_df.isna().apply(lambda row: np.any(row), axis=1)
        na_numbers = np.sum(index_labels)
        if self.logger:
            self.logger.info("rows with na features: {}, percentage {}".format(na_numbers, 1.0 * na_numbers / total_samples))
        if self.input_feature_type == "train":
            self.feature_df.dropna(axis=0, inplace=True)
        else:
            # for prediction, fill in na features by 0
            # TODO: in future we need to fill in na featrues in a more reasonable way
            self.feature_df.fillna(0, inplace=True)
        self.feature_df.reset_index(drop=True, inplace=True)

    def postprocessing_features(self):
        """
        feature postprocessing function.
        remove those features that have the same value
        TODO:add the function that removing those features that have the strong correlation in case of colinearity
        :return:
        """
        exclude_column = self.key_columns + [self.target_column]
        remove_column = []
        for col in self.feature_df:
            if col in exclude_column:
                continue
            values = self.feature_df[col].values
            if len(np.unique(values)) == 1:
                remove_column.append(col)
                self.feature_df.drop([col], axis=1, inplace=True)
                self.feature_list.remove(col)
        if len(remove_column) > 0:
            if self.logger:
                self.logger.info("Remove the following columns that has unique values.")
                self.logger.info(remove_column)


# using the model as features selector
class FeatureSelector(object):
    """
    Irrelevant or partially relevant features can negatively impact model performance.
    using different methods to select important features:
    1. Model selection: using the randomforest model or lassocv model to do the feature selection
    2. Univariate Selection: calculate the correlation between feature and target, then select
    3. Variace threhold method: the default method
    the specified the top N features
    """
    def __init__(self, selector=None, input_feature_type="train", feature_select_mode=1, trained_model=None,
                 valid_feature=None, variance_thd=0.1, feature_best_num=None, logger=None):
        self.selector = selector
        self.input_feature_type = input_feature_type
        self.feature_select_mode = feature_select_mode
        self.trained_model = trained_model
        self.valid_feature = valid_feature
        self.variance_threshold = variance_thd
        self.feature_best_num = feature_best_num
        self.logger = logger

    def fit(self, df):
        return self

    def transform(self, data_dict):
        if self.logger:
            self.logger.info("...FeatureSelect...")
        if self.input_feature_type == 'test':
            if self.selector is None:
                raise ValueError('when doing test feature engineering, the selector should provide !')
        feature_df = data_dict["feature"]
        target_array = data_dict["target"]
        if self.selector is None:
            if self.feature_select_mode == 1:
                if self.feature_best_num is None:
                    self.feature_best_num = int(feature_df.columns.unique().__len__()*0.8)   # the default ratio is 80%
                selector = SelectKBest(score_func=f_regression, k=self.feature_best_num)
                transformed_feature = selector.fit_transform(feature_df, target_array)
            elif self.feature_select_mode == 2:
                if self.trained_model is None:
                    for feature_select_model in ["rf-reg"]:
                        self.logger.info("Use model {} for feature selection".format(feature_select_model))
                        if feature_select_model == "rf-reg":
                            selector = SelectFromModel(RandomForestRegressor())
                        else:
                            selector = SelectFromModel(LassoCV())
                else:
                    selector = SelectFromModel(self.trained_model)
                    transformed_feature = selector.fit_transform(feature_df, target_array)
            else:
                selector = VarianceThreshold(threshold=self.variance_threshold)
                transformed_feature = selector.fit_transform(feature_df)
        else:
            # using the provide selector when doing test
            selector = self.selector

        feature_flag = selector.get_support()
        all_features = feature_df.columns
        selected_features = all_features[feature_flag]
        removed_features = all_features[np.logical_not(feature_flag)]
        self.logger.info("Total removed features {}".format(len(removed_features)))
        self.logger.info(removed_features.values)
        self.logger.info("Remaining features {}".format(len(selected_features)))
        self.logger.info(selected_features)

        return {"feature": feature_df[selected_features], 'target': target_array,
                "selector": selector, 'time': data_dict['time']}

    @staticmethod
    def normalize_features(df, columns):
        for column in columns:
            # remove nan values
            array = np.array([value for value in np.array(df[column], dtype=float) if not math.isnan(value)])
            df[column] = (df[column]-array.min())/(array.max()-array.min())
        return df





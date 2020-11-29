import numpy as np
import pandas as pd
from core.datahandle import DataHandler
from workalendar.asia import China  # 节假日计算包
import warnings
from core.config import Config

warnings.filterwarnings('ignore')

class FeatureEngineer():
    """
    特征工程，包含：
    滑窗特征，时间特征（包含傅里叶变换），组特征
    """
    def __init__(self, handled_data, id_column, time_column, target_column, group_columns, predict_target, predict_period):
        """
        指定分类列名、时间列名、预测目标列名，求组平均特征的组分类列名，预测周期（天/周/月）
        :param config:
        """
        self.handled_data = handled_data
        self.id_column = id_column
        self.time_column = time_column
        self.target_column = target_column
        self.group_columns = group_columns
        self.predict_target = predict_target
        self.predict_period = predict_period

    def feature_data_transform(self):
        """
        将特征合并为一个dataframe，注意：组平均特征的获取需要先生成时间特征
        :param self:
        :return:
        """
        shift_feature_df = self.lag_feature(lag_point_list=list(range(3, 8)))  # 滑窗特征
        time_feature_df = self.time_feature() # 时间特征
        fourier_series_feature_df = self.fourier_series() # 傅里叶变换特征
        holiday_feature_df = self.holiday_feature() # 节假日信息特征
        sales_date_1 = self.sales_feature(date_from = '20200618', date_end = '20200622')
        sales_date_2 = self.sales_feature(date_from = '20200818', date_end = '20200822')
        sales_date = pd.concat([sales_date_1, sales_date_2], axis = 0, ignore_index=True)
        print('XXXX:')
        print(sales_date)

        feature_df = pd.concat([self.handled_data, shift_feature_df, time_feature_df, fourier_series_feature_df], axis=1)
                                                                            # 将除组平均、节假日、促销日的特征合并
        group_feature_df = self.group_feature(feature_df)

        feature_df = pd.concat([feature_df, group_feature_df], axis = 1) # 与组平均特征合并

        feature_df.dropna(axis=0, inplace=True) # 将做滑窗空的行删去
        feature_df.reset_index(inplace=True, drop=True) # 删除后重新index
        feature_df = pd.merge(feature_df, holiday_feature_df, how='left', on=[self.time_column]) # 节假日合并
        feature_df = pd.merge(feature_df, sales_date, left_on = 'date', right_on = 'is_sale', how = 'left')
        feature_df['is_sale']= feature_df['is_sale'].notnull() + 0

        feature_df['festival'] = feature_df['festival'].notnull() + 0 # 节假日0、1转换

        feature_df['target'] = feature_df[self.predict_target]  # 定义需要预测的列

        feature_df.drop(self.target_column, axis=1, inplace=True) # 将当日的收/派件列删去
        feature_df.drop('year', axis=1, inplace=True)  # 均为2020年数据，无用

        feature_df.sort_values(by = [self.id_column, self.time_column], ascending=(True, True), inplace = True, ignore_index = True)

        feature_df.to_csv('predict_feature/{}_feature.csv'.format(self.predict_target), index = True)

        return feature_df

    def lag_feature(self, lag_point_list = list(range(3,8))):
        """
        周度规律性较强，取一个周期内的滑窗；由于要一次提交三天的预测结果，故取预测天前7~3天的历史记录
        :return:
        """
        shift_feature_df = pd.DataFrame()

        for target_column in self.target_column:
            df_zone = pd.DataFrame()
            for zone_id, groups in self.handled_data.groupby(self.id_column):
                df_zone_shift = pd.DataFrame() # 每一个zone的滑窗结果
                zone_value = groups[[target_column]]  # DataFrame，index保持不变
                zone_columns = list()
                for  i, shift_num in enumerate(lag_point_list):
                    col_name = '{}_shift_{}'.format(target_column, shift_num)
                    zone_columns.append(col_name)
                    cur_series = zone_value.shift(shift_num) # 滑窗，index不会跟着移动
                    cur_series.rename({ target_column: col_name}, axis=1, inplace=True)
                    df_zone_shift = pd.concat([df_zone_shift, cur_series], axis=1) # 每滑动一次，加入一列的结果

                df_zone = pd.concat([df_zone, df_zone_shift], axis=0)  # 将各zone的结果竖向拼接起来

            shift_feature_df = pd.concat([shift_feature_df, df_zone], axis = 1) # 将shou/pai滑窗的结果横向拼接起来

        shift_feature_df.reset_index(drop=True, inplace=True) # 重新index

        return shift_feature_df

    def time_feature(self):
        """
        取时间特征
        :return:
        """
        time_series = self.handled_data[self.time_column]
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
        time_feature_df = pd.DataFrame(date_normal_features, columns=date_normal_columns)  # index未改变过

        return time_feature_df

    def fourier_series(self):
        """
        时间的傅里叶变换
        :return:
        """
        if self.predict_period == 'yearly':
            period = 365.245
            series_order = 1
        elif self.predict_period == 'weekly':
            period = 7
            series_order = 5
        elif self.predict_period == 'daily':
            period = 1
            series_order = 3

        time_series = self.handled_data[self.time_column]
        t = np.array(
            (time_series - pd.datetime(1970, 1, 1))
                .dt.total_seconds()
                .astype(np.float)
        ) / (3600 * 24.)

        value = np.column_stack(
            [fun((2.0 * (i + 1) * np.pi * t / period)) for i in range(series_order) for fun in (np.sin, np.cos)])

        daily_columns = ['daily_delim_{}'.format(i + 1) for i in range(value.shape[1])]
        fourier_series_feature_df = pd.DataFrame(value, columns=daily_columns)

        return fourier_series_feature_df

    def group_feature(self, time_handled_df):
        """
        组平均特征：按各zone求平均,注：为防止数据暴露，舍去最后三天的数据
        :return:
        """
        group_feature_df = pd.DataFrame()
        for zone, zone_data in time_handled_df.groupby(self.id_column):
            zone_data.reset_index(drop=True, inplace=True)
            zone_feature_df = pd.DataFrame()

            for target_column in self.target_column:

                for group_target in self.group_columns:
                    group_features = np.zeros((len(zone_data), 5))
                    for id, groups in zone_data.groupby(group_target):
                        if len(groups[target_column]) > 3:
                            value_arr = groups[target_column][:-3].values   # 舍去最后三天的数据
                            stats_vec = [np.max(value_arr), np.min(value_arr), np.median(value_arr), np.std(value_arr),
                                        np.sum(value_arr)]
                            assign_index = zone_data[zone_data[group_target] == id].index
                            group_features[assign_index, :] = stats_vec
                        elif len(groups[target_column]) <= 3:
                            value_arr = value_arr   # 若某月不足三条数据，取上一月的数据
                            stats_vec = [np.max(value_arr), np.min(value_arr), np.median(value_arr), np.std(value_arr),
                                         np.sum(value_arr)]
                            assign_index = zone_data[zone_data[group_target] == id].index
                            group_features[assign_index, :] = stats_vec

                    group_column = ["{}_groupby_{}_{}".format(target_column, group_target, i) for i in
                                    ["max", "min", "median", "std", "sum"]]
                    group_feature = pd.DataFrame(group_features, columns=group_column,
                                                 index=list(range(0, len(zone_data))))
                    zone_feature_df = pd.concat([zone_feature_df, group_feature], axis=1)

            group_feature_df = pd.concat([group_feature_df, zone_feature_df], axis=0)

        group_feature_df.reset_index(drop=True, inplace=True)

        return group_feature_df

    def holiday_feature(self):
        """
        获取节假日信息
        :return:
        """
        def cal_festival(year):
            cal = China()
            lis = []
            if type(year) != list:  # eval函数就是实现list、dict、tuple与str之间的转化
                year = eval(year)
            for ye in year:
                for x, v in cal.holidays(ye):
                    lis.append([str(x).replace('-', ''), v])
            df = pd.DataFrame(data=lis, columns = [self.time_column, 'festival'])
            return df

        holiday_feature_df = cal_festival([2020])
        dt_format = '%Y%m%d'
        holiday_feature_df[self.time_column] = pd.to_datetime(holiday_feature_df[self.time_column], format=dt_format)

        return holiday_feature_df

    def sales_feature(self, date_from, date_end):
        """
        标注促销信息，6.18~6.22受618促销的影响，销量增加，考虑到提交日期包含8.18，此处加入促销特征
        :return:
        """
        dt_format = '%Y%m%d'
        date_from = pd.to_datetime(date_from, format = dt_format)
        date_end = pd.to_datetime(date_end, format = dt_format)
        date_range = pd.date_range(start = date_from, end = date_end, freq = 'D')

        sales_date = pd.Series(date_range, name = 'is_sale')

        return sales_date

if __name__ == '__main__':
    from core.model import model_feature_engineer, model_data_split
    dataset = DataHandler('../data/quantity_train.csv', id_column = Config.id_column,
                                                    time_column = Config.time_column,
                                                    target_column = Config.target_column)
    handled_data = dataset.data_transform()

    model_data = model_data_split(handled_data, Config.id_column, Config.time_column, 21)
    model_feature = model_feature_engineer(model_data)
    model_feature.to_csv('model_feature.csv')













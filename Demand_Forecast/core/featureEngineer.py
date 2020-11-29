# -*- coding: utf-8 -*-
"""
Created on Thu Nov  5 21:29:39 2020

@author: Administrator
"""
import pandas as pd

class FeatureEngineer():
    """
    特征工程
    """
    def __init__(self, df, config):
        """
        共有变量及属性
        """
        self.df = df
        self.config = config
        self.melt_data = pd.DataFrame() #训练集数据（特征工程后）
        self.melt_test_data = pd.DataFrame() #测试集数据（特征工程后）
        self.category_index_num = self.config.feature_start_index  #前几列是类别特征
        self.feature_loop_index = self.config.feature_loop_index #取前几个月的数据进行训练
        self.one_category_feature_num = self.config.one_category_feature_num #每一组（价格or销售额包含的月份数）

    def create_statistic_feature(self, df, feature_loop_index, prefix=''):
        """
        均值、中位数、标准差、0销售额月份
        """
        df[prefix + '_mean'] = df.iloc[:, 0:feature_loop_index].mean(axis=1)
        df[prefix + '_median'] = df.iloc[:, 0:feature_loop_index].median(axis=1)
        df[prefix + '_std'] = df.iloc[:, 0:feature_loop_index].std(axis=1)
        df[prefix + '_zero'] = (df.iloc[:, 0:feature_loop_index] == 0).astype(int).sum(axis=1)
        return df

    def porudct_sum(self, df1, df2, feature_loop_index):
        """
        各shop_id、iten_id在前三个月中，每月的销售额
        """
        sum_product = pd.DataFrame([])
        for i in range(feature_loop_index):
            product = df1.iloc[:, i] * df2.iloc[:, i]
            if i == 0:
                sum_product = product
            else:
                sum_product = pd.concat([sum_product, product], axis=1)
        sum_product.columns = [f'profit_{i}' for i in range(feature_loop_index)]
        return sum_product

    def feature_transform(self, start_index, end_index, data_type = 'train'):
        """
        feature_engineer Work
        :return:
        """
        one_catgory_feature_num = self.one_category_feature_num
        feature_loop_index = self.feature_loop_index
        sales_per_month = self.df
        feature_start_index = self.category_index_num

        loop_times = 0
        for i in range(start_index, end_index):
            id = sales_per_month.iloc[:, 0:feature_start_index]  # 首先提取ID
            id.columns = ['shop_id', 'item_id', 'item_category_id']
            sales = sales_per_month.iloc[:, feature_start_index + i:feature_start_index + feature_loop_index + i]
            print(sales.shape)
            sales.columns = [f'sales_{i}' for i in range(feature_loop_index)]  # 提取销售额
            price = sales_per_month.iloc[:, feature_start_index + i + one_catgory_feature_num: \
                                            feature_start_index + feature_loop_index + i + one_catgory_feature_num]
            price.columns = [f'price_{i}' for i in range(feature_loop_index)]  # 提取价格
            sales = self.create_statistic_feature(sales, feature_loop_index, prefix='sales')  # 提取销售的统计特征
            price = self.create_statistic_feature(price, feature_loop_index, prefix='price')  # 提取价格的统计特征
            total_profit = self.porudct_sum(sales, price, feature_loop_index)  # 提取销售额
            price['year'] = list(sales_per_month.columns)[feature_start_index + feature_loop_index + i -1][1]  # 提取年份
            price['month'] = list(sales_per_month.columns)[feature_start_index + feature_loop_index + i -1][2]  # 提取月份
            if price['month'][0] < 12:
                price['month'] = price['month'] +1
            else:
                price['year'] = price['year'] + 1
                price['month'] = 1
            if data_type == 'train':
                target = sales_per_month.iloc[:, feature_start_index + feature_loop_index + i]  # 提取target
                target.name = 'target'
                sample = pd.concat([id, price, sales, total_profit, target], axis=1)  # 合并所有特征
            elif data_type == 'test':
                sample = pd.concat([id, price, sales, total_profit], axis=1)
            if loop_times == 0:
                melt_data = sample
            else:
                melt_data = pd.concat([melt_data, sample], axis=0)  # 融合所有的样本组

            loop_times += 1

        print(melt_data.info())

        return melt_data

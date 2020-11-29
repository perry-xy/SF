# -*- coding: utf-8 -*-
"""
Created on Thu Nov  5 21:29:39 2020

@author: Administrator
"""
import pandas as pd
import numpy as np


class DataHandler():
    """
    处理数据：读取数据、整合数据、处理日期、数据透视
    """

    def __init__(self, filepath):
        """
        共有变量及属性
        :param filepath:
        """
        self.filepath = filepath
        self.raw_df = pd.DataFrame()  # 训练集数据：销量、价格等
        self.category_df = pd.DataFrame()  # 商品品类归属，category_id的具体含义
        self.item_df = pd.DataFrame()  # 商品信息
        self.shop_df = pd.DataFrame()  # 商店信息
        self.final_test_df = pd.DataFrame()  # 需要提交的预测数据，包含商店和item

        self.all_info = pd.DataFrame()  # 训练集数据与item的merge结果，获取item的category
        self.full_shop_item_matrix = pd.DataFrame()  # shop与item、category的笛卡尔积
        self.sales_per_month = pd.DataFrame()  # 特征工程之前的最终数据集合；做好了透视及shop、item的合并

        self.load_data()
        self.time_handle()
        self.merge_transform()

    def load_data(self):
        """
        读取数据
        :param self:
        :return:
        """
        self.raw_df = pd.read_csv('{}/sales_train.csv'.format(self.filepath))
        self.final_test_df = pd.read_csv('{}/test.csv'.format(self.filepath))
        self.category_df = pd.read_csv('{}/item_categories.csv'.format(self.filepath))
        self.item_df = pd.read_csv('{}/items.csv'.format(self.filepath))
        self.shop_df = pd.read_csv('{}/shops.csv'.format(self.filepath))

    def time_handle(self):
        """
        对日期做一些处理，转换与构造
        :param self:
        :return:
        """
        # 整合数据、处理日期
        dt_format = '%d.%m.%Y'
        self.raw_df['date'] = pd.to_datetime(self.raw_df['date'], format=dt_format)
        self.all_info_df = pd.merge(left=self.raw_df, right=self.item_df, on=['item_id'], how='left')
        print(self.all_info_df.info())
        self.all_info_df.drop(['item_name'], axis=1, inplace=True)
        print(self.all_info_df.info())

        # 获取时间细节
        def time_period_features(df, time_col):
            # month level
            df['month'] = df[time_col].dt.month
            # quarter level
            df['quarter'] = df[time_col].dt.quarter
            # year level
            df['year'] = df[time_col].dt.year
            return df

        self.all_info_df = time_period_features(self.all_info_df, 'date')
        print(self.all_info_df.info())

    def merge_transform(self):
        """
        transform：做成数据透视表
        merge：取所有shop与item的笛卡尔积(应题目要求）
        :param self:
        :return:
        """
        # 做数据透视表：按shop_id,item_id,item_category_id分组，统计销售额、销售价格在各年、月份的统计特征
        self.sales_per_month = pd.pivot_table(
            data=self.all_info_df, values=['item_cnt_day', 'item_price'],
            index=['shop_id', 'item_id', 'item_category_id'],
            columns=['year', 'month'],
            aggfunc={'item_cnt_day': 'sum', 'item_price': 'mean'})
        print(self.sales_per_month.info())

        self.sales_per_month.fillna(0, inplace=True)
        self.sales_per_month.reset_index(inplace=True)  # 将shop_id、item_id、category_id展开成列
        print(self.sales_per_month.info())

        # 取商店与SKU的笛卡尔积
        all_items = self.item_df[['item_id', 'item_category_id']]
        for shop_id in self.shop_df['shop_id'].values:
            all_items_per_shop = all_items.copy()
            all_items_per_shop['shop_id'] = shop_id
            if self.full_shop_item_matrix.shape[0] == 0:
                self.full_shop_item_matrix = all_items_per_shop
            else:
                self.full_shop_item_matrix = pd.concat([self.full_shop_item_matrix, all_items_per_shop], axis=0)

        print(self.full_shop_item_matrix.shape)

        # 将变量的类型转为float32,降低内存占用
        for col in list(self.sales_per_month.columns)[3:]:
            self.sales_per_month[col] = self.sales_per_month[col].astype(np.float32)
        print(self.sales_per_month.info())

        # 合并所有的'shop、item、category组合'与已有销售数据
        self.sales_per_month = pd.merge(left=self.sales_per_month, right=self.full_shop_item_matrix,
                                        left_on=[('shop_id', '', ''), ('item_id', '', ''),
                                                 ('item_category_id', '', '')], \
                                        right_on=['shop_id', 'item_id', 'item_category_id'],
                                        how='right')

        self.sales_per_month.drop(['shop_id', 'item_id', 'item_category_id'], inplace=True, axis=1)
        print(self.sales_per_month.info())
        self.sales_per_month.fillna(value=0, inplace=True)
        print(self.sales_per_month.info())







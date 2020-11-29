# -*- coding: utf-8 -*-
"""
Created on Thu Nov  5 21:29:39 2020

@author: Administrator
"""
import pandas as pd
import lightgbm as lgb
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
import numpy as np
import matplotlib.pyplot as plt
#from bayes_opt import BayesianOptimization
import csv
from hyperopt import STATUS_OK
from hyperopt import hp
from hyperopt import tpe
from hyperopt import Trials
from hyperopt import fmin

class LightGBM_Demand():
    """
    一个LightGBM进行预测的简单框架
    """
    def __init__(self,filepath):
        """
        共有变量及属性
        :param filepath:
        """
        self.filepah = filepath

file = 'predict_future_sales'

"""
读取电商销售数据
"""
raw_df = pd.read_csv('{}/sales_train.csv'.format(file))
final_test_df = pd.read_csv('{}/test.csv'.format(file))
category_df = pd.read_csv('{}/item_categories.csv'.format(file))
item_df = pd.read_csv('{}/items.csv'.format(file))
shop_df = pd.read_csv('{}/shops.csv'.format(file))

"""
整合数据、处理日期、数据透视
"""
# 整合数据、处理日期
dt_format = '%d.%m.%Y'
raw_df['date'] = pd.to_datetime(raw_df['date'], format=dt_format)
all_info_df = pd.merge(left=raw_df, right=item_df, on=['item_id'], how='left')
print(all_info_df.info())
all_info_df.drop(['item_name'], axis=1, inplace=True)
print(all_info_df.info())
print(all_info_df.columns)

# 获取时间细节
def time_period_features(df, time_col):
    # month level
    df['month'] = df[time_col].dt.month
    # quarter level
    df['quarter'] = df[time_col].dt.quarter
    # year level
    df['year'] = df[time_col].dt.year
    return df

all_info_df = time_period_features(all_info_df, 'date')
print(all_info_df.info())

# 做数据透视表：按shop_id,item_id,item_category_id分组，统计销售额、销售价格在各年、月份的统计特征
sales_per_month = pd.pivot_table(
    data=all_info_df, values=[
        'item_cnt_day', 'item_price'], index=[
            'shop_id', 'item_id', 'item_category_id'], columns=[
                'year', 'month'], aggfunc={
                    'item_cnt_day': 'sum', 'item_price': 'mean'})
print(sales_per_month.info())

sales_per_month.fillna(0, inplace=True)
sales_per_month.reset_index(inplace=True) #将shop_id、item_id、category_id展开成列
print(sales_per_month.info())

"""
据结果数据，可看出要求每种sku在各shop的月销量，而销售数据中，并未给出所有的shop与item组合，而仅有产生过销量的，
此处给出所有的组合
"""
# 取商店与SKU的笛卡尔积
full_shop_item_matrix = pd.DataFrame([])
all_items = item_df[['item_id','item_category_id']]
for shop_id in shop_df['shop_id'].values:
    all_items_per_shop = all_items.copy()
    all_items_per_shop['shop_id'] = shop_id
    if full_shop_item_matrix.shape[0] ==0:
        full_shop_item_matrix = all_items_per_shop
    else:
        full_shop_item_matrix = pd.concat([full_shop_item_matrix, all_items_per_shop], axis=0)

print(full_shop_item_matrix.shape)

# 将变量的类型转为float32,降低内存占用
for col in list(sales_per_month.columns)[3:]:
    sales_per_month[col] = sales_per_month[col].astype(np.float32)
print(sales_per_month.info())
# 合并所有的'shop、item、category组合'与已有销售数据
sales_per_month = pd.merge(left=sales_per_month,right= full_shop_item_matrix,
                           left_on=[('shop_id','',''),('item_id','',''),('item_category_id','','')],\
                           right_on = ['shop_id','item_id','item_category_id'],
                           how='right')

sales_per_month.drop(['shop_id', 'item_id','item_category_id'], inplace=True, axis=1)
print(sales_per_month.info())
sales_per_month.fillna(value=0, inplace=True)
print(sales_per_month.info())

"""
特征工程：①前三个月销量/价格的均值、中位数、标准差、数值为0的月份；
②前三个月每月的销售额
"""
# 定义函数
def create_statistic_feature(df,feature_loop_index,prefix = ''):
    """
    均值、中位数、标准差、0销售额月份
    """
    df[prefix+'_mean']=df.iloc[:,0:feature_loop_index].mean(axis=1)
    df[prefix+'_median']=df.iloc[:,0:feature_loop_index].median(axis=1)
    df[prefix+'_std']=df.iloc[:,0:feature_loop_index].std(axis=1)
    df[prefix+'_zero'] =(df.iloc[:,0:feature_loop_index] == 0).astype(int).sum(axis=1)
    return df

def porudct_sum(df1,df2,feature_loop_index):
    """
    各shop_id、iten_id在前三个月中，每月的销售额
    """
    sum_product = pd.DataFrame([])
    for i in range(feature_loop_index):
        product = df1.iloc[:,i]*df2.iloc[:,i]
        if i ==0 :
            sum_product = product
        else:
            sum_product = pd.concat([sum_product,product],axis=1)
    sum_product.columns = [f'profit_{i}' for i in range(feature_loop_index)]
    return sum_product

# 特征构造
melt_data = pd.DataFrame([]) #创建一个空df
print(sales_per_month.shape)
feature_start_index = 3 # 应为shop_id、item_id、category_id不会迭代变化
feature_loop_index = 3 # 设定采用几个月的数据作为feature，你可以修改，如果是33表示所有的过去的月份
one_catgory_feature_num = 34 # 每组（价格或者销售额）包含了34 个月份

for i in range(one_catgory_feature_num - feature_loop_index):
    id = sales_per_month.iloc[:, 0:feature_start_index]  #首先提取ID
    id.columns = ['shop_id','item_id','item_category_id']
    sales = sales_per_month.iloc[:, feature_start_index + i:feature_start_index + feature_loop_index + i]
    print(sales.shape)
    sales.columns = [f'sales_{i}' for i in range(feature_loop_index)]  # 提取销售额
    price = sales_per_month.iloc[:, feature_start_index + i + one_catgory_feature_num:\
                                    feature_start_index + feature_loop_index + i + one_catgory_feature_num]
    price.columns = [f'price_{i}' for i in range(feature_loop_index)] # 提取价格
    sales = create_statistic_feature(sales, feature_loop_index, prefix='sales') # 提取销售的统计特征
    price = create_statistic_feature(price, feature_loop_index, prefix='price') # 提取价格的统计特征
    total_profit = porudct_sum(sales, price, feature_loop_index)  # 提取销售额
    price['year'] = list(sales_per_month.columns)[feature_start_index + i][1]  # 提取年份
    price['month'] = list(sales_per_month.columns)[feature_start_index + i][2] # 提取月份
    target = sales_per_month.iloc[:, feature_loop_index + i + 1] # 提取target
    target.name = 'target'
    sample = pd.concat([id, price, sales, total_profit,target], axis=1) #合并所有特征
    if i == 0:
        melt_data = sample
    else:
        melt_data = pd.concat([melt_data, sample], axis=0) # 融合所有的样本组
print(melt_data.info())

# 所需预测结果集特征构造
id = sales_per_month.iloc[:, 0:feature_start_index]
id.columns = ['shop_id','item_id','item_category_id']
sales = sales_per_month.iloc[:, feature_start_index +
                             one_catgory_feature_num -
                             feature_loop_index:feature_start_index +
                             one_catgory_feature_num]
sales.columns = [f'sales_{i}' for i in range(feature_loop_index)]
price = sales_per_month.iloc[:, feature_start_index +
                             one_catgory_feature_num +
                             one_catgory_feature_num -
                             feature_loop_index:feature_start_index +
                             one_catgory_feature_num +
                             one_catgory_feature_num]
price.columns = [f'price_{i}' for i in range(feature_loop_index)]
sales = create_statistic_feature(sales, feature_loop_index, prefix='sales')
price = create_statistic_feature(price, feature_loop_index, prefix='price')
total_profit= porudct_sum(sales,price,feature_loop_index)
price['year'] = 2015
price['month'] = 11

melt_test_df = pd.concat([id, price, sales,total_profit], axis=1)
print(melt_test_df.info())

"""
构造训练数据集，并用默认参数构造baseline
"""
# 构造数据集
y = melt_data['target']
X = melt_data.drop('target', axis=1)
print(X.info())

train_X, test_X, train_y, test_y = train_test_split(
    X, y, test_size=0.3, random_state=123)

dtrain = lgb.Dataset(train_X.values, label=train_y.values)
dtest = lgb.Dataset(test_X.values, label=test_y.values)

valid_sets = [dtrain, dtest]
valid_name = ['train', 'eval']
feature_name = list(X.columns)

# # 构造训练baseline
# evals_result = {}
# default_params = lgb.LGBMRegressor().get_params()
# metric = 'mape'
# default_params['metric'] = [metric]
# default_params['objective'] = 'regression'
# early_stop = int(0.1 * default_params['n_estimators'])
# model = lgb.train(params = default_params,
#                   train_set = dtrain,
#                   feature_name=feature_name,
#                   valid_sets=valid_sets,
#                   valid_names=valid_name,
#                   evals_result=evals_result,
#                   early_stopping_rounds= early_stop
#                   )
# fig, ax = plt.subplots()
# ax.plot(evals_result['train'][metric], label='Train')
# ax.plot(evals_result['eval'][metric], label='Test')
# ax.legend()
# plt.ylabel(f'{metric}')
# plt.title(f'lightGBM {metric}')
# plt.show()
# if len(evals_result['eval'][metric]) == default_params['n_estimators']:
#     default_mape = evals_result['eval'][metric][-1]
# else:
#     default_mape = evals_result['eval'][metric][-(early_stop+1)]
#
# print('默认参数下LGB在测试集上的mape为:'+str(default_mape))

"""
贝叶斯优化调参
"""
metric = 'mape'
evals_result = {}
# 定义优化函数
def objective(hyperparameters):
    """
    定义目标函数，需返回递减的loss，或'loss'键后面是递减的score
    :param hyperparameters:
    :return:
    """
    global ITERATION, OUT_FILE

    ITERATION += 1

    if 'n_estimators' in hyperparameters:
        del hyperparameters['n_estimators']

    subsample = hyperparameters['boosting_type'].get('subsample', 1.0)

    hyperparameters['boosting_type'] = hyperparameters['boosting_type']['boosting_type']
    hyperparameters['subsample'] = subsample

    for parameter_name in ['num_leaves', 'max_depth']:#['num_leaves', 'subsample_for_bin', 'min_child_samples']:
        hyperparameters[parameter_name] = int(hyperparameters[parameter_name])

    hyperparameters['metric'] = [metric]

    cv_results = lgb.train(hyperparameters,
                           dtrain,
                           num_boost_round=1000,
                           early_stopping_rounds= 20,
                           feature_name=feature_name,
                           valid_sets=valid_sets,
                           valid_names=valid_name,
                           evals_result=evals_result,
                           )
    if len(evals_result) == 1000 :
        loss = evals_result['eval'][metric][-1]
    else:
        loss = evals_result['eval'][metric][-21]

    n_estimators = len(evals_result['eval'][metric])

    hyperparameters['n_estimators'] = n_estimators

    of_connection = open(OUT_FILE, 'a')
    writer = csv.writer(of_connection)
    writer.writerow([loss, hyperparameters, ITERATION])
    of_connection.close()

    return {'loss': loss, 'hyperparameters': hyperparameters, 'iteration': ITERATION,
             'status': STATUS_OK}

# 定义参数空间
space = {
    'boosting_type': hp.choice('boosting_type',
     [{'boosting_type': 'gbdt', 'subsample': hp.uniform('gdbt_subsample', 0.5, 1)},
     {'boosting_type': 'dart', 'subsample': hp.uniform('dart_subsample', 0.5, 1)},
     {'boosting_type': 'goss', 'subsample': 1.0}]),
    'num_leaves': hp.quniform('num_leaves', 20, 150, 1),
    'learning_rate': hp.uniform('learning_rate', 0.01, 0.5),
    #'subsample_for_bin': hp.quniform('subsample_for_bin', 20000, 300000, 20000),
    #'min_child_samples': hp.quniform('min_child_samples', 20, 500, 5),
    'reg_alpha': hp.uniform('reg_alpha', 0.0, 1.0),
    'reg_lambda': hp.uniform('reg_lambda', 0.0, 1.0),
    'colsample_bytree': hp.uniform('colsample_by_tree', 0.6, 1.0),
    'max_depth': hp.quniform('max_depth', 4, 10, 1)
    #'is_unbalance': hp.choice('is_unbalance', [True, False]),
}

# 优化
trials = Trials()
ITERATION = 0
OUT_FILE = 'bayes_test.csv'
of_connection = open(OUT_FILE, 'w')
writer = csv.writer(of_connection)
# Write column names
headers = ['loss', 'hyperparameters', 'iteration', 'status']
writer.writerow(headers)
of_connection.close()

best = fmin(fn = objective, space=space, algo = tpe.suggest, trials=trials, max_evals=100)
print('最佳参数为：\n')
print(best)
print('最佳结果为: \n')
trials_dict = sorted(trials.results, key = lambda x: x['loss'])
print(trials_dict[:1])





# 贝叶斯调参
# 调节的参数：boosting,max_depth,eta,num_leaves,lambda_l1,lambda_l2
# # 参数空间
# param_grid = {
#     #'boosting': ['gbdt', 'goss', 'dart'],
#     'max_depth': (4,10),
#     'eta':(0.005,0.5),
#     'num_leaves': (20,150),
#     'n_iter': (100,1100),
#     'lambda_l1':(0.05,1),
#     'lambda_l2':(0.05,1),}
    #'learning_rate': list(np.logspace(np.log10(0.005), np.log10(0.5), base = 10, num = 1000)),
    #'subsample_for_bin': list(range(20000, 300000, 20000)),
    #'min_child_samples': list(range(20, 500, 5)),
    # 'reg_alpha': list(np.linspace(0, 1)),
    # 'reg_lambda': list(np.linspace(0, 1)),
    # 'colsample_bytree': list(np.linspace(0.6, 1, 10)),
    # 'subsample': list(np.linspace(0.5, 1, 100)),
    #'is_unbalance': [True, False]
# }
# # 定义优化目标值
# def bayes_opt_obj(max_depth,eta,num_leaves,n_iter,lambda_l1,lambda_l2,boosting='goss'):
#     evals_result = {}
#     metric = 'mape'
#     param = {'boosting':boosting,
#              'max_depth':int(max_depth),
#              'eta':eta,
#              'num_leaves':int(num_leaves),
#              'objective':'regression',
#              'verbose': 0,
#              'metric': [metric],
#              'lambda_l1':lambda_l1,
#              'lambda_l2':lambda_l2
#     }
#     model = lgb.train(params = param,
#                     train_set = dtrain,
#                     num_boost_round = int(n_iter),
#                     feature_name=feature_name,
#                     valid_sets=valid_sets,
#                     valid_names=valid_name,
#                     evals_result=evals_result,
#                     early_stopping_rounds=20,
#     )
#     if len(evals_result['eval'][metric]) == n_iter:
#         return - evals_result['eval'][metric][-1]
#     else:
#         return - evals_result['eval'][metric][-20]
#
# lgb_op = BayesianOptimization(
#         bayes_opt_obj,
#         param_grid
#     )
#
# lgb_op.maximize()
# print(lgb_op.max)










# param = {
#     'boosting':'goss',
#     'max_depth': 6,
#     'eta': 0.01,
#     'num_leaves':40,
#     'objective': 'regression',
#     'verbose': 0,
#     'metric': ['mape'],
# }
# evals_result = {}
# valid_sets = [dtrain, dtest]
# valid_name = ['train', 'eval']
# feature_name = list(X.columns)
# #
# model = lgb.train(
#     param,
#     dtrain,
#     num_boost_round=1000,
#     feature_name=feature_name,
#     valid_sets=valid_sets,
#     valid_names=valid_name,
#     evals_result=evals_result,
#     early_stopping_rounds=20)
# metric = 'mape'
# fig, ax = plt.subplots()
# ax.plot(evals_result['train'][metric], label='Train')
# ax.plot(evals_result['eval'][metric], label='Test')
# ax.legend()
# plt.ylabel(f'{metric}')
# plt.title(f'lightGBM {metric}')
# plt.show()
#
# if acc_month == True :
#     print("精确月收益时，MAPE为："+str(evals_result['eval'][metric][-1]))
# elif month == True:
#     print("三月总和时，MAPE为：" + str(evals_result['eval'][metric][-1]))
# else:
#     if len(evals_result['eval'][metric]) == 1000:
#         print("goss + 精确月时，MAPE为：" + str(evals_result['eval'][metric][-1]))
#     else:
#         print("goss + 精确月时，MAPE为：" + str(evals_result['eval'][metric][-20]))

# import lightgbm as lgb
from sklearn import datasets
from sklearn.model_selection import KFold
from sklearn.model_selection import cross_val_score
from sklearn import datasets

#from bayes_opt.util import Colours






# y_hat = model.predict(X.values)
# fig, ax = plt.subplots(2, 1)
# ax[0].plot(y_hat)
# ax[1].plot(y.values)

# melt_test_df['target'] = model.predict(melt_test_df.values)
# # melt_test_df.reset_index(inplace=True)
# melt_test_df['target'].plot()
# result = pd.merge(
#     final_test_df,
#     melt_test_df,
#     left_on=[
#         'shop_id',
#         'item_id'],
#     right_on=[
#         'shop_id',
#         'item_id'],
#     how='left')
# print(result.isna().any())
# print(result.info())
# # result.fillna(0, inplace=True)
# # result['ID'] = result.index
# result.rename({'target': 'item_cnt_month'}, inplace=True, axis=1)
# result['item_cnt_month'].clip(0,20,inplace=True)
#
# result[['ID', 'item_cnt_month']].to_csv('submission.csv', index=False)

























































import csv
import ast

# datahandle
# def aoi_handle(self):
#     """
#     关于aoi属性的处理：
#     取各zone历史日期中最大的发生收派件的aoi数目、aoi类型数、aoi面积总和作为aoi的属性
#     :return:
#     """
#     aoi_collects = pd.pivot_table(data=self.origin_data,
#                                   index=self.id_column,
#                                   columns=self.time_column,
#                                   values=['aoi_area', 'aoi_id', 'aoi_type'],
#                                   aggfunc={'aoi_area': 'sum',
#                                            'aoi_id': 'count',
#                                            'aoi_type': lambda x: len(x.unique())})
#     aoi_collects.fillna(0, inplace=True)
#     aoi_collects = aoi_collects.stack()
#     aoi_collects = aoi_collects.unstack(level=0)
#     aoi_collects = aoi_collects.max()
#     aoi_collects = aoi_collects.unstack(level=0)
#     aoi_collects.columns = ['area', 'aoi_num', 'aoi_type']
#     aoi_collects.reset_index(inplace=True)
#
#     return aoi_collects
#
# # 贝叶斯优化
# def bayes_optimize_objective(hyperparameters):
#     """
#     构造贝叶斯优化的目标函数
#     :return:
#     """
#     Config.bayes_iterations += 1
#     # if 'n_estimators' in hyperparameters:
#     #     del hyperparameters['n_estimators']
#
#     subsample = hyperparameters['boosting_type'].get('subsample', 1.0)  # 若没有subsample这个键时，返回1.0;
#                                                                         # 采用'goss'优化方法时，subsample只能为1
#     hyperparameters['boosting_type'] = hyperparameters['boosting_type']['boosting_type']
#     hyperparameters['subsample'] = subsample
#
#     for parameter_name in ['num_leaves', 'max_depth', 'n_estimators']:  # 必须去取整的参数
#         hyperparameters[parameter_name] = int(hyperparameters[parameter_name])
#
#     hyperparameters['metric'] = Config.metric
#
#     evals_result = {}
#     opti_model = lgb.train(hyperparameters,
#                              train_set = Config.bayes_dtrain,
#                              early_stopping_rounds = int(0.1 * hyperparameters['n_estimators']),
#                              feature_name = Config.bayes_feature_name,
#                              valid_sets = [Config.bayes_dtrain, Config.bayes_dtest],
#                              valid_names = ['train', 'eval'],
#                              evals_result = evals_result,
#                              categorical_feature = Config.categorical_feature
#                              )
#
#     loss = opti_model.best_score['eval'][Config.metric[0]]
#     if len(evals_result['eval'][Config.metric[0]]) != hyperparameters['n_estimators']: # 如果发生了earlying_stop，取earlying_stop时的学习器个数
#         hyperparameters['n_estimators'] = opti_model.best_iteration
#
#     of_connection = open('{}/zone{}_bayes_test.csv'.format(Config.out_filepath, Config.out_zone), 'a')
#     writer = csv.writer(of_connection)
#     writer.writerow([loss, hyperparameters, Config.bayes_iterations, STATUS_OK]) # 写入本次迭代的参数
#     of_connection.close()
#
#     return {'loss': loss, 'hyperparameters': hyperparameters, 'iteration': Config.bayes_iterations, 'status': STATUS_OK}
#
# def bayes_optimization(opti_type = 'all', max_evals = 10):
#     """
#     贝叶斯优化的过程
#     :return:
#     """
#     trials = Trials() # 记录迭代结果
#     of_connection = open('{}/zone{}_bayes_test.csv'.format(Config.out_filepath, Config.out_zone), 'w')
#     writer = csv.writer(of_connection)
#     headers = ['loss', 'hyperparameters', 'iteration', 'status'] # 写入行名称
#     writer.writerow(headers)
#     of_connection.close()
#
#     if opti_type == 'all':
#         best = fmin(fn = bayes_optimize_objective, space = Config.space_all, algo = tpe.suggest, trials = trials, max_evals = max_evals)
#     elif opti_type == 'zone':
#         best = fmin(fn=bayes_optimize_objective, space = Config.space_zone, algo=tpe.suggest, trials=trials, max_evals=max_evals)
#
#     print('最佳参数为：\n')
#     print(best)
#     print('最佳结果为: \n')
#     trials_dict = sorted(trials.results, key=lambda x: x['loss'])  # 嵌套字典
#     df = pd.DataFrame.from_dict(trials_dict)
#     print('SSSSS:')
#     print(df)
#     df.to_csv('aaa.csv',index = False)
#     print(trials_dict[:1])
#
#     results = pd.read_csv('{}/zone{}_bayes_test.csv'.format(Config.out_filepath, Config.out_zone))
#     bayes_params, hyp_df = bayes_evaluate(results, name = 'LGB_Model')
#     print(hyp_df)
#
#     hyp_df.to_csv('{}/zone{}_best_iteration.csv'.format(Config.out_filepath, Config.out_zone))
#
#     return bayes_params
#
# def bayes_evaluate(results, name):
#     """
#     整理贝叶斯优化的中间结果，输出：best_iteration文件，其将params字典拆分为各列
#     :param results:
#     :param name:
#     :param metric:
#     :return:
#     """
#     new_results = results.copy()
#     new_results['hyperparameters'] = new_results['hyperparameters'].map(ast.literal_eval)
#
#     new_results = new_results.sort_values('loss', ascending=True).reset_index(drop=True)
#
#     print('The best perform from {} was {:.5f} found on iteration {}.'.format(name, new_results.loc[0, 'loss'],
#                                                                                     new_results.loc[0, 'iteration']))
#
#     hyperparameters = new_results.loc[0, 'hyperparameters']  # 最佳的参数
#
#     hyp_df = pd.DataFrame(columns=list(new_results.loc[0, 'hyperparameters'].keys()))
#
#     for i, hyp in enumerate(new_results['hyperparameters']):
#         hyp_df = hyp_df.append(pd.DataFrame(hyp, index=[0]), ignore_index=True)  # 将参数字典拆解为DataFrame形式
#
#     hyp_df['iteration'] = new_results['iteration']
#     hyp_df['metric'] = Config.metric[0]
#
#     return hyperparameters, hyp_df
#
# 作图
# fig, ax = plt.subplots()
# ax.plot(bayes_evals_result['train'][Config.metric[0]], label='Train')
# ax.plot(bayes_evals_result['eval'][Config.metric[0]], label='Test')
# ax.legend()
# plt.ylabel(f'{Config.metric[0]}')
# plt.title(f'lightGBM {Config.metric[0]}')
# plt.savefig("altogether_bayes_model_img/image_altogether.png")
#
# # 划分训练集验证集；划分zone
# @staticmethod
#     def train_valid_split(train_X, train_y, id_column, data_type, train_size = 0.8):
#         """
#         split the train dataset to train set and validation set;
#         the ratio is 8:2
#         :param df:
#         :param id_column:
#         :param time_coulmn:
#         :return:
#         """
#         train_X_df = pd.DataFrame()
#         train_y_df = pd.Series()
#         vali_X_df = pd.DataFrame()
#         vali_y_df = pd.Series()
#         if data_type == 'all':
#             train_long = int(train_size * len(train_X)/30) # zone number = 30
#         else:
#             train_long = int(train_size * len(train_X))
#
#         for id, groups in train_X.groupby(id_column):
#             train_X_group = groups.iloc[0:train_long,:]
#             train_y_group = train_y[train_X_group.index]
#             vali_X_group = groups.iloc[train_long:,:]
#             vali_y_group = train_y[vali_X_group.index]
#
#             train_X_df = pd.concat([train_X_df, train_X_group], axis=0)
#             train_y_df = pd.concat([train_y_df, train_y_group], axis=0)
#             vali_X_df = pd.concat([vali_X_df, vali_X_group], axis=0)
#             vali_y_df = pd.concat([vali_y_df, vali_y_group], axis=0)
#
#         train_y_df.name = 'target'
#         vali_y_df.name = 'target'
#
#         train_X_df.reset_index(drop=True, inplace = True)
#         train_y_df.reset_index(drop=True, inplace=True)
#         vali_X_df.reset_index(drop=True, inplace = True)
#         vali_y_df.reset_index(drop=True, inplace=True)
#
#         return {'opti_train_X':train_X_df,'opti_train_y': train_y_df,
#                 'opti_vali_X':vali_X_df,'opti_vali_y': vali_y_df}
#
#     @staticmethod
#     def zone_split(train_dataset, zone_id, id_column, time_column):
#         """
#         split the train data by the zone_id when training independently on every zone is necessary
#         :return:
#         """
#         train_X, train_y, test_X, test_y = train_dataset['train_X'], train_dataset['train_y'], \
#                                            train_dataset['test_X'], train_dataset['test_y']
#
#         dataset_columns = ["{}_{}_zone{}".format(i, j, str(zone_id))
#                            for i in ["train", "test"]
#                            for j in ['X', 'y']]
#
#         locals()[dataset_columns[0]] = train_X.loc[train_X['zone_id'] == zone_id, :]
#         train_zone_index = train_X[train_X['zone_id'] == zone_id].index
#         locals()[dataset_columns[1]] = train_y[train_zone_index]
#         locals()[dataset_columns[0]].reset_index(drop=True, inplace=True)
#         locals()[dataset_columns[1]].reset_index(drop=True, inplace=True)
#
#         locals()[dataset_columns[2]] = test_X.loc[test_X['zone_id'] == zone_id, :]
#         test_zone_index = test_X[test_X['zone_id'] == zone_id].index
#         locals()[dataset_columns[3]] = test_y[test_zone_index]
#         locals()[dataset_columns[2]].reset_index(drop=True, inplace=True)
#         locals()[dataset_columns[3]].reset_index(drop=True, inplace=True)
#
#         return {'train_X': locals()[dataset_columns[0]],
#                 'train_y': locals()[dataset_columns[1]],
#                 'test_X': locals()[dataset_columns[2]],
#                 'test_y': locals()[dataset_columns[3]]}

# 区分zone建模
# else:   # 区分zone训练
#
#     if Model_Select_Config.params_optimization == False: # 区分训练 & 不进行参数优化
#         """
#         若不进行参数优化，直接按7轮进行训练，输出近似泛化误差
#         """
#         split_result = list() # 预测平均mape
#         mape_results_df = pd.DataFrame() # 预测mape
#         test_results_df = pd.DataFrame() # 预测结果
#         feature_importance_df = pd.DataFrame() # 特征重要性
#         for zone in range(0,30): # 30个zone
#             zone_df = feature_df.loc[feature_df[Config.id_column] == zone, :]
#             zone_df.reset_index(drop = True, inplace = True)
#
#             test_mape, test_mape_result, test_results_unit, feature_importance_unit, model \
#                                                     = model_evaluate(zone_df, params=Config.default_params, test_rounds=7, test_period=3)
#             print("21天交叉验证后的泛化误差为：{}".format(str(test_mape)))
#             split_result.append(test_mape)
#             index = ['7.21-7.23', '7.24-7.26', '7.27-7.29', '7.30-8.1', '8.1-8.4', '8.5-8.7', '8.8-8.10']
#             zone_no = [zone for i in range(7) ]
#             mape_df = pd.DataFrame({'zone': zone_no, 'date': index, 'mape': test_mape_result})
#             mape_results_df = pd.concat([mape_results_df, mape_df], axis = 0)  # mape_df
#             test_results_df = pd.concat([test_results_df, test_results_unit], axis = 0) # test_results_df
#             feature_importance_unit.index = [zone for i in range(len(feature_importance_unit))]
#             feature_importance_df = pd.concat([feature_importance_df, feature_importance_unit], axis = 0) # feature_importance_df
#
#         print('区分zone建模的平均{}为:{}'.format(Config.metric[0], str(pd.Series(split_result).mean())))
#         mape_results_df.to_csv(
#                     'results/{}/non_params_optimization/Mape_test_result.csv'.format(Model_Select_Config.predict_target), index = False)
#         test_results_df.to_csv(
#             'results/{}/non_params_optimization/test_results.csv'.format(Model_Select_Config.predict_target), index=False)
#         feature_importance_df.to_csv(
#         'results/{}/non_params_optimization/feature_importance.csv'.format(Model_Select_Config.predict_target), index=True)
#
#     elif Model_Select_Config.params_optimization == True:
#         split_result = list()
#         mape_results_df = pd.Data.Frame()  # 预测mape
#         test_results_df = pd.DataFrame()  # 预测结果
#         feature_importance_df = pd.DataFrame()  # 特征重要性
#         for zone in range(0,30):
#             Config.out_zone = str(zone)
#
#             zone_df = feature_df.loc[feature_df[Config.id_column] == zone, :]
#             zone_df.reset_index(drop=True, inplace=True)
#
#             zone_dataset = FeatureEngineer.train_test_split(zone_df, id_column=Config.id_column,
#                                                                time_column=Config.time_column,
#                                                                test_period=21, predict_period=21)  # 测试集取最后21天
#             train_data = pd.concat([zone_dataset['train_X'], zone_dataset['train_y']], axis=1)  # 训练集总数据，还会划分为训练&验证集
#
#             Config.bayes_data = train_data
#             Config.test_period = 3
#             Config.test_rounds = 7
#
#             best_params, bayes_process_df = bayes_optimization(opti_type='zone', max_evals=10)
#
#             # 测试集上迭代7次
#             test_mape, test_mape_result, test_results_unit, feature_importance_unit, model = \
#                                                             model_evaluate(zone_df, params=best_params, test_rounds=7, test_period=3)
#             model.save_model('model_zone{}.txt'.format(str(zone)))
#             print("21天交叉验证后的泛化误差为：{}".format(str(test_mape)))
#             print(best_params)
#             index = ['7.21-7.23', '7.24-7.26', '7.27-7.29', '7.30-8.1', '8.1-8.4', '8.5-8.7', '8.8-8.10']
#
#             zone_no = [zone for i in range(7)]
#             mape_df = pd.DataFrame({'zone': zone_no, 'date': index, 'mape': test_mape_result})
#             mape_results_df = pd.concat([mape_results_df, mape_df], axis=0)  # mape_df
#             test_results_df = pd.concat([test_results_df, test_results_unit], axis=0)  # test_results_df
#             feature_importance_unit.index = [zone for i in range(len(feature_importance_unit))]
#             feature_importance_df = pd.concat([feature_importance_df, feature_importance_unit],
#                                               axis=0)  # feature_importance_df
#
#         print('区分zone建模的平均{}为:{}'.format(Config.metric[0], str(pd.Series(split_result).mean())))
#         pd.Series(test_mape_result, index=index, name=Config.metric[0]).to_csv(
#                 'results/{}/params_optimization/Mape_test_result.csv'.format(Model_Select_Config.predict_target))
#         test_results_df.to_csv(
#                 'results/{}/params_optimization/test_results.csv'.format(Model_Select_Config.predict_target),
#                 index=False)
#         feature_importance_df.to_csv(
#                 'results/{}/params_optimization/feature_importance.csv'.format(Model_Select_Config.predict_target),
#                 index=False)
#         bayes_process_df.to_csv('results/{}/params_optimization/zone{}_bayes_process.csv'. \
#                                     format(Model_Select_Config.predict_target, Config.out_zone), index=False)

# # 训练集测试集划分
# @staticmethod
#     def train_test_split(df, id_column, time_column, test_period, predict_period):
#         """
#         训练集与验证/测试集划分;
#         在参数优化时，先取n-21天的数据进行训练集/验证集划分，确定最优参数后，再取所有数据进行训练集/测试集划分
#         :param df:
#         :return:
#         """
#         df.sort_values(by = [id_column, 'month', 'day'], ascending=(True, True, True), inplace = True, ignore_index = True)
#
#         train_X_df = pd.DataFrame()
#         train_y_df = pd.Series()
#         test_X_df = pd.DataFrame()
#         test_y_df = pd.Series()
#
#         for id, groups in df.groupby(id_column):   # 以zone_id划分
#             train_X_group = groups.iloc[:-predict_period , :-1]
#             train_y_group = groups.iloc[:-predict_period , -1]   # 取除最后predict_period天的数据为训练集
#             if predict_period != test_period:
#                 test_X_group = groups.iloc[-predict_period : -(predict_period - test_period) , :-1]
#                 test_y_group = groups.iloc[-predict_period : -(predict_period - test_period) , -1]
#             else:
#                 test_X_group = groups.iloc[-predict_period: , :-1]
#                 test_y_group = groups.iloc[-predict_period: , -1]   # 取训练集后test_predict长的数据为测试集
#
#             train_X_df = pd.concat([train_X_df, train_X_group], axis=0)
#             train_y_df = pd.concat([train_y_df, train_y_group], axis=0)
#             test_X_df = pd.concat([test_X_df, test_X_group], axis=0)
#             test_y_df = pd.concat([test_y_df, test_y_group], axis=0) # 每一个zone纵向拼接
#
#         if time_column in train_X_df.columns:
#             train_X_df.drop(columns = time_column, axis = 1, inplace = True)
#             test_X_df.drop(columns = time_column, axis = 1, inplace = True)
#
#         train_X_df.reset_index(drop=True, inplace = True)
#         train_y_df.reset_index(drop=True, inplace=True)
#         test_X_df.reset_index(drop=True, inplace = True)
#         test_y_df.reset_index(drop=True, inplace=True)  # 重新index，将test数据的index也变为从0开始
#
#         train_y_df.name = 'target'
#         test_y_df.name = 'target'
#
#         print('训练集中的Feature为：\n')
#         print(train_X_df.columns)
#
#         return {'train_X':train_X_df, 'train_y':train_y_df, 'test_X':test_X_df, 'test_y':test_y_df}

# 组平均特征1201版
# group_feature_df = pd.DataFrame()
#         for zone, zone_data in time_handled_df.groupby(self.id_column):
#             zone_data.reset_index(drop=True, inplace=True)
#             zone_data = zone_data.iloc[:-3,:]
#             zone_feature_df = pd.DataFrame()
#
#             for target_column in self.target_column:
#
#                 for group_target in self.group_columns:
#                     group_features = np.zeros((len(zone_data), 5))
#                     for id, groups in zone_data.groupby(group_target):
#                         value_arr = groups[target_column]
#                         stats_vec = [np.max(value_arr), np.min(value_arr), np.median(value_arr), np.std(value_arr),
#                                    np.sum(value_arr)]
#                         assign_index = zone_data[zone_data[group_target] == id].index
#                         group_features[assign_index, :] = stats_vec
#                         # if len(groups[target_column]) > 3:
#                         #     value_arr = groups[target_column][:-3].values   # 舍去最后三天的数据
#                         #     stats_vec = [np.max(value_arr), np.min(value_arr), np.median(value_arr), np.std(value_arr),
#                         #                 np.sum(value_arr)]
#                         #     assign_index = zone_data[zone_data[group_target] == id].index
#                         #     group_features[assign_index, :] = stats_vec
#                         # elif len(groups[target_column]) <= 3:
#                         #     value_arr = value_arr   # 若某月不足三条数据，取上一月的数据
#                         #     stats_vec = [np.max(value_arr), np.min(value_arr), np.median(value_arr), np.std(value_arr),
#                         #                  np.sum(value_arr)]
#                         #     assign_index = zone_data[zone_data[group_target] == id].index
#                         #     group_features[assign_index, :] = stats_vec
#
#                     group_column = ["{}_groupby_{}_{}".format(target_column, group_target, i) for i in
#                                     ["max", "min", "median", "std", "sum"]]
#                     group_feature = pd.DataFrame(group_features, columns=group_column,
#                                                  index=list(range(0, len(zone_data))))
#                     zone_feature_df = pd.concat([zone_feature_df, group_feature], axis=1)
#
#             group_feature_df = pd.concat([group_feature_df, zone_feature_df], axis=0)
#
#         group_feature_df.reset_index(drop=True, inplace=True)
#
#         return group_feature_df
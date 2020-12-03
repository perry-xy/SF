import lightgbm as lgb
from hyperopt import tpe
from hyperopt import Trials
from hyperopt import fmin
from hyperopt import STATUS_OK
import pandas as pd
from core.config import Config
from core.feature_engineer import FeatureEngineer
import numpy as np
from core.datahandle import DataHandler
import matplotlib.pyplot as plt

"""
数据集划分的function:
训练集与验证/测试集划分; 在参数优化时，先取n-21天的数据进行训练集/验证集划分，确定最优参数后，再取所有数据进行训练集/测试集划分
"""
def model_data_split(handled_data, id_column, time_column, predict_period):
    """
    参数优化7轮交叉验证，测试集上7轮交叉验证，每次往前多取3天的数据，通过调整predict_period进行控制
    :param df:
    :return:
    """
    handled_data.sort_values(by=[id_column, time_column], ascending=(True, True), inplace=True, ignore_index=True)

    model_data = pd.DataFrame()    # 进入此次训练的数据

    for id, groups in handled_data.groupby(id_column):  # 以zone_id划分
        if predict_period != 0:
            zone_data = groups.iloc[:-predict_period, :]  # 取除最后predict_period天的数据为训练集
            model_data = pd.concat([model_data, zone_data], axis=0)  # 每一个zone纵向拼接
        else:
            zone_data = groups.iloc[:, :]
            model_data = pd.concat([model_data, zone_data], axis=0)

    model_data.reset_index(drop=True, inplace=True)

    return model_data

def model_feature_engineer(model_data):
    """"
    在model_data确定之后，在数据集上进行特征工程
    """
    feature_input = FeatureEngineer(model_data, id_column = Config.id_column, time_column = Config.time_column,
                                                predict_period='daily',target_column = Config.target_column,
                                                group_columns = ['weekday', 'month'],
                                                predict_target = Config.predict_target)
    model_feature = feature_input.feature_data_transform()

    return model_feature

def model_train_test_split(model_feature, id_column, time_column, test_period = 3):
    """
    每次取最后三天为测试集，注：每个zone的最后三天
    :param test_period:
    :return:
    """
    model_feature.sort_values(by=[Config.id_column, Config.time_column], ascending=(True, True),
                                                                         inplace=True, ignore_index=True)

    train_X_df = pd.DataFrame()
    train_y_df = pd.Series()
    test_X_df = pd.DataFrame()
    test_y_df = pd.Series()

    for id, groups in model_feature.groupby(id_column):  # 以zone_id划分
        train_X_group = groups.iloc[: -test_period, :-1]
        train_y_group = groups.iloc[: -test_period, -1]
        test_X_group = groups.iloc[-test_period :, :-1]
        test_y_group = groups.iloc[-test_period :, -1]   # 以最后三天进行划分

        train_X_df = pd.concat([train_X_df, train_X_group], axis=0)
        train_y_df = pd.concat([train_y_df, train_y_group], axis=0)
        test_X_df = pd.concat([test_X_df, test_X_group], axis=0)
        test_y_df = pd.concat([test_y_df, test_y_group], axis=0)  # 每一个zone纵向拼接

    if time_column in train_X_df.columns:
        train_X_df.drop(columns=time_column, axis=1, inplace=True)
        test_X_df.drop(columns=time_column, axis=1, inplace=True)  # 删除日期列

    train_X_df.reset_index(drop=True, inplace=True)
    train_y_df.reset_index(drop=True, inplace=True)
    test_X_df.reset_index(drop=True, inplace=True)
    test_y_df.reset_index(drop=True, inplace=True)  # 重新index，将test数据的index也变为从0开始

    train_y_df.name = 'target'
    test_y_df.name = 'target'

    print('训练集中的Feature为：\n')
    print(train_X_df.columns)

    return {'train_X': train_X_df, 'train_y': train_y_df, 'test_X': test_X_df, 'test_y': test_y_df}

"""
关于模型训练与调参的function
"""

def model_train(dataset, categorical_feature, objective, metric, params = Config.default_params):
    """
    给定参数后LGB模型的训练，不指定early_stopping，不指定测试集
    :return:
    """
    dtrain = lgb.Dataset(dataset['train_X'].values, label = dataset['train_y'].values,
                                                    categorical_feature = categorical_feature)

    # predict baseline
    evals_result = {}
    params['metric'] = metric                 #默认参数中无metric，指定一下
    params['objective'] = objective        #默认参数中无objective，指定一下
    if params['objective'] == 'quantile':
        params['alpha'] = Config.alpha     #如果是分位数回归，指定分位数点
    valid_sets = [dtrain]
    valid_name = ['train']
    print('ZZZZ:')
    print(params)
    model = lgb.train(params = params,
                      train_set = dtrain,
                      feature_name = list(dataset['train_X'].columns),
                      valid_sets = valid_sets,
                      valid_names = valid_name,
                      evals_result = evals_result,
                      categorical_feature = categorical_feature
                      )

    iterations = params['n_estimators']
    default_mape = model.best_score['train'][metric[0]]

    print('训练集上的{}为:{}'.format(metric[0], str(default_mape)))

    return model, iterations, default_mape, evals_result

"""
贝叶斯优化
"""
def plot_iterations(bayes_evals_result, test_rounds):
    """
    绘制参数优化过程的模型训练迭代图
    :return:
    """
    fig, ax = plt.subplots()
    ax.plot(bayes_evals_result['train'][Config.metric[0]], label='Train')
    ax.plot(bayes_evals_result['eval'][Config.metric[0]], label='Test')
    ax.legend()
    plt.ylabel(f'{Config.metric[0]}')
    plt.title(f'lightGBM {Config.metric[0]}')
    plt.savefig('iterations_image/{}_iterations_rounds{}.png'.format(Config.img_savepath, test_rounds))

def bayes_optimize_objective(hyperparameters):
    """
    构造贝叶斯优化的目标函数
    :return:
    """
    Config.bayes_iterations += 1  # 记录搜寻次数

    for parameter_name in ['num_leaves', 'max_depth', 'n_estimators']:  # 必须去取整的参数
        hyperparameters[parameter_name] = int(hyperparameters[parameter_name])

    hyperparameters['metric'] = Config.metric
    hyperparameters['objective'] = Config.objective

    loss_list = []
    if hyperparameters['n_estimators'] <= 200:
        earlying_stop = 30
    else:
        earlying_stop = int(0.1 * hyperparameters['n_estimators'])

    for i in range(Config.test_rounds):  # 取交叉验证的平均误差作为调参依据

        evals_result = {}
        model_data = model_data_split(Config.bayes_data, id_column=Config.id_column, time_column=Config.time_column,
                                                        predict_period=(Config.test_rounds * Config.test_period) +
                                                                        (Config.test_rounds - i -1) * Config.test_period)
        model_feature = model_feature_engineer(model_data)
        eval_dataset = model_train_test_split(model_feature, id_column=Config.id_column, time_column=Config.time_column,
                                                                                            test_period=3)   # 划分数据
        dtrain = lgb.Dataset(eval_dataset['train_X'].values, label=eval_dataset['train_y'].values, categorical_feature=
                                                                                           Config.categorical_feature)
        dtest = lgb.Dataset(eval_dataset['test_X'].values, label=eval_dataset['test_y'].values, categorical_feature=
                                                                                            Config.categorical_feature)
        opti_model = lgb.train(hyperparameters, train_set = dtrain, valid_sets = [dtrain, dtest], valid_names = ['train', 'eval'],
                                                feature_name = list(eval_dataset['train_X'].columns),
                                                evals_result=evals_result,
                                                early_stopping_rounds = earlying_stop, #int(0.1 * hyperparameters['n_estimators']),
                                                categorical_feature = Config.categorical_feature)  # 模型训练

        loss_list.append(opti_model.best_score['eval'][Config.metric[0]])  # metric误差
        plot_iterations(evals_result, i+1)

    if len(evals_result['eval'][Config.metric[0]]) != hyperparameters['n_estimators']:
        hyperparameters['n_estimators'] = opti_model.best_iteration  # 如果发生了earlying_stop，取earlying_stop时的学习器个数
    loss = pd.Series(loss_list).mean() # 平均误差

    return {'loss': loss, 'hyperparameters': hyperparameters, 'iteration': Config.bayes_iterations, 'status': STATUS_OK}

def bayes_optimization(max_evals=10):
    """
    贝叶斯优化的过程
    :return:
    """
    trials = Trials()  # 记录迭代结果

    best = fmin(fn=bayes_optimize_objective, space=Config.space, algo=tpe.suggest, trials=trials,
                    max_evals=max_evals)

    print('最佳参数为：\n')
    print(best)
    print('最佳结果为: \n')
    trials_dict = sorted(trials.results, key=lambda x: x['loss'])  # 嵌套字典
    print(trials_dict)
    print(trials_dict[:1])

    best_params = trials_dict[:1][0]['hyperparameters'] # 最佳参数

    bayes_process_df = pd.DataFrame.from_dict(trials_dict)

    return best_params, bayes_process_df

def mape(true_y, predict_y):
    """
    计算测试集上的mape，参数均为np.array
    :param true_y:
    :param predict_y:
    :return:
    """
    true_y = np.array(true_y)
    return (abs(predict_y - true_y)/true_y).mean()

def model_evaluate(handled_data, params, test_rounds = 7, test_period = 3):
    """
    21天滚动预测，用于验证集上误差评估/测试集上泛化性能评估
    :param test_X:
    :param test_y:
    :param test_rounds:
    :return:
    """
    test_mape_result = []  # 储存每一个3天的结果
    test_results = pd.DataFrame() # 储存每个3天预测的结果
    train_results = pd.DataFrame() # 储存训练集上的预测结果
    feature_importance_df = pd.DataFrame()
    for i in range(test_rounds):
        model_data = model_data_split(handled_data, id_column = Config.id_column, time_column = Config.time_column,
                                                                    predict_period = test_period * (test_rounds - i -1))
        model_feature = model_feature_engineer(model_data)
        eval_dataset = model_train_test_split(model_feature, id_column = Config.id_column,
                                              time_column = Config.time_column, test_period = 3)

        model, iterations, train_mape, evals_result = model_train(eval_dataset,
                                                            categorical_feature = Config.categorical_feature,
                                                            objective = Config.objective,
                                                            metric = Config.metric,
                                                            params = params)  # 模型训练

        unit_test_result = model.predict(eval_dataset['test_X'].values)  # 模型预测
        unit_test_result_s = pd.Series(unit_test_result, name='predict')
        unit_test_result_s = unit_test_result_s.apply(lambda x : round(x))
        test_total_results = pd.concat([eval_dataset['test_X'], eval_dataset['test_y'],
                                                                 unit_test_result_s], axis=1) # 与特征一起构成DataFrame
        test_total_results['zone_id'] = test_total_results['zone_id'].map(lambda x: 'zone_' + str(x)) # 将zone_id变为zone_n形式
        test_results = pd.concat([test_results, test_total_results], axis = 0) # 整合预测结果（7轮一起）

        test_mape = mape(eval_dataset['test_y'], unit_test_result) # 计算单次预测mape
        test_mape_result.append(test_mape)

        feature_importance_unit = pd.DataFrame({'column': list(eval_dataset['train_X'].columns),
                                              'importance': model.feature_importance()},
                                        index = ['round'+str(i) for x in range(len(eval_dataset['train_X'].columns))]
                                                                            ).sort_values(by='importance', ascending=False)
        feature_importance_df = pd.concat([feature_importance_df, feature_importance_unit], axis = 0)

    # 最后一次训练集上的结果
    unit_train_result = model.predict(eval_dataset['train_X'].values)  # 模型预测
    unit_train_result_s = pd.Series(unit_train_result, name='predict')
    unit_train_result_s = unit_train_result_s.apply(lambda x: round(x))
    train_total_results = pd.concat([eval_dataset['train_X'], eval_dataset['train_y'],
                                     unit_train_result_s], axis=1)  # 与特征一起构成DataFrame
    train_total_results['zone_id'] = train_total_results['zone_id'].map(lambda x: 'zone_' + str(x))
    # train_results = pd.concat([train_results, train_total_results, test_total_results], axis=0)
    train_results = pd.concat([train_results, train_total_results],axis = 0)

    # 对train_results的结果重排下序
    train_results.sort_values(by=[Config.id_column, 'month', 'day'], ascending=(True, True, True), inplace=True, ignore_index=True)

    train_results.reset_index(drop = True, inplace = True) # 从0开始
    test_results.reset_index(drop = True, inplace = True) # 从0开始

    return pd.Series(test_mape_result).mean(), test_mape_result, test_total_results, feature_importance_df, model, train_results

if __name__ == '__main__':
    data_input = DataHandler('data/quantity_train.csv', id_column=Config.id_column, time_column=Config.time_column,
                             target_column=Config.target_column)
    handled_data = data_input.data_transform()  # 做好处理的数据

    model_data = model_data_split(handled_data, Config.id_column, Config.time_column, 21)

    model_feature = model_feature_engineer(model_data)

    # model_feature.to_csv('model_feature.csv')
    eval_dataset = model_train_test_split(model_feature, Config.id_column, Config.time_column)
    eval_dataset['train_X'].to_csv('train_X.csv')
    eval_dataset['test_X'].to_csv('test_X.csv')
    print(eval_dataset['train_X'].shape)
    print(eval_dataset['train_X'].index)
    print(eval_dataset['train_y'].shape)
    print(eval_dataset['train_y'].index)
    print(eval_dataset['test_X'].shape)
    print(eval_dataset['test_X'].index)
    print(eval_dataset['test_y'].shape)
    print(eval_dataset['test_y'].index)






    











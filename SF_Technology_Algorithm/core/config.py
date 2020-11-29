from hyperopt import hp
import numpy as np
import lightgbm as lgb

class Config:
    """
    模型调节参数
    """
    """
    损失函数、id、时间、目标、类别变量全局通用定义
    """
    # 模型训练全局变量
    metric = ['mape']
    objective = 'regression'  # quantile
    alpha = None              # 分位数点
    id_column = 'zone_id'
    time_column = 'date'
    target_column = ['pai_num', 'shou_num']
    categorical_feature = ['zone_id', 'is_weekend', 'is_month_start', 'is_month_end', 'is_quarter_start',
                           'is_quarter_end',
                           'festival', 'is_sale']
    predict_target = 'pai_num'

    """
    贝叶斯优化过程中一些需全局定义的记录变量
    """
    # ①贝叶斯迭代次数记录变量，在函数内，需为全局变量；②贝叶斯优化过程中的迭代图储存路径
    bayes_iterations = 0
    img_savepath = None

    # 贝叶斯优化过程中的数据、交叉验证的轮数、每轮测试的时间窗
    bayes_data = None
    test_rounds = None
    test_period = None

    """
    参数搜索空间
    """
    # 总体训练优化空间
    space = {
        'num_leaves': hp.quniform('num_leaves', 11, 300, 1),
        'learning_rate': hp.uniform('learning_rate', 0.005, 0.4),
        'reg_alpha': hp.uniform('reg_alpha', 0.0, 1.0),
        'reg_lambda': hp.uniform('reg_lambda', 0.0, 1.0),
        'colsample_bytree': hp.uniform('colsample_by_tree', 0.6, 1.0),
        'max_depth': hp.quniform('max_depth', 2, 10, 1),
        'n_estimators': hp.quniform('n_estimators', 50, 200, 10)
    }

    # 默认参数空间
    default_params = lgb.LGBMRegressor().get_params()
    # {'boosting_type': 'gbdt',
    #  'class_weight': None,
    #  'colsample_bytree': 1.0,
    #  'importance_type': 'split',
    #  'learning_rate': 0.1,
    #  'max_depth': -1,
    #  'min_child_samples': 20,
    #  'min_child_weight': 0.001,
    #  'min_split_gain': 0.0,
    #  'n_estimators': 100,
    #  'n_jobs': -1,
    #  'num_leaves': 31,
    #  'objective': None,
    #  'random_state': None,
    #  'reg_alpha': 0.0,
    #  'reg_lambda': 0.0,
    #  'silent': True,
    #  'subsample': 1.0,
    #  'subsample_for_bin': 200000,
    #  'subsample_freq': 0}




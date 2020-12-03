from core.datahandle import DataHandler
from core.model import bayes_optimization, model_evaluate
from core.config import Config
import pandas as pd
import os

class Model_Select_Config():
    """
    选择总体训练/分zone训练，参数优化/不进行参数优化
    """
    predict_target = 'shou_num'     # pai_num / shou_num
    model_type = 'all'         # True: 所有Zone一起训练；False：区分zone训练
    params_optimization = True      # 是否进行参数优化
    use_all_feature = True        # True: 预测收/派时，用收&派的滑窗、组平均特征；False：仅用自己的
    quantile_regression = False   # True：分位数回归；False：均值回归
    if use_all_feature:
        Config.target_column = ['pai_num', 'shou_num']#, 'sf_pai_num', 'sf_shou_num']
        # Config.target_column = ['pai_num', 'shou_num', 'sf_pai_num', 'sf_shou_num']
    else:
        Config.target_column = [predict_target]
    if quantile_regression:
        Config.objective = 'quantile'
        Config.alpha = 0.76   # 分位数点
    else:
        Config.objective = 'regression'
    outfile_path = 'adjustment_result_save'

Config.predict_target = Model_Select_Config.predict_target

data_input = DataHandler('data/quantity_train.csv', id_column = Config.id_column, time_column = Config.time_column,
                         target_column = Config.target_column)
handled_data = data_input.data_transform()  # 做好预处理的数据

date = '1204'
pre_handled_data = DataHandler.predict_handle('data/{}_predict_input.csv'.format(date))

handled_data = pd.concat([handled_data, pre_handled_data], axis = 0)
handled_data.sort_values(by = [Config.id_column, Config.time_column], ascending = (True,True), inplace = True)
handled_data.reset_index(drop = True, inplace = True)

if Model_Select_Config.model_type == 'all': # 如果总体训练

    if Model_Select_Config.params_optimization == False: # 总体训练 & 不进行参数优化
        """
        若不进行参数优化，直接按7轮进行训练，输出近似泛化误差
        """
        test_mape, test_mape_result, test_results, feature_importance_df, model, train_results \
                                    = model_evaluate(handled_data, params = Config.default_params, test_rounds = 5  ,
                                                                                                    test_period = 3)
        print(test_mape_result)
        print("15天交叉验证后的泛化误差为：{}".format(str(test_mape)))
        index = [ '8.5-8.7', '8.8-8.10', '8.11-8.13', '8.14-8.16','8.17-8.19']

        if not os.path.exists('{}_results/'.format(str(Model_Select_Config.outfile_path))):        # 为本次结果建一个文件夹
            os.mkdir('{}_results'.format(str(Model_Select_Config.outfile_path)))
        output_path = '{}_results'.format(str(Model_Select_Config.outfile_path))         # 文件路径

        pd.Series(test_mape_result, index = index, name = Config.metric[0]).to_csv('{}/{}_{}_Mape_test_result_noopti.csv'
            .format(output_path, date,  Model_Select_Config.predict_target))                        # 7轮迭代的mape
        train_results.to_csv('{}/{}_{}_train_results_noopti.csv'
            .format(output_path, date, Model_Select_Config.predict_target),index=False)            # 最后一轮迭代的整体预测结果
        test_results.to_csv('{}/{}_{}_test_results_noopti.csv'
            .format(output_path, date, Model_Select_Config.predict_target), index = False)         # 7轮迭代的预测值
        feature_importance_df.to_csv('{}/{}_{}_feature_importance_noopti.csv'
            .format(output_path, date, Model_Select_Config.predict_target), index = True)          # 7轮迭代的feature_importance

    elif Model_Select_Config.params_optimization == True: # 总体训练 & 参数优化
        """
        若需要参数优化，则在训练集 & 验证集上进行调参，用测试集输出近似泛化误差
        """
        Config.bayes_data = handled_data
        Config.test_period = 3  # 每次的验证集为3天
        Config.test_rounds = 5  # 验证7轮
        Config.img_savepath = Model_Select_Config.predict_target[:3]  # 迭代图保存至哪一个路径下，pai/shou

        # best_params, bayes_process_df = bayes_optimization(max_evals = 50)   # 搜索100次
        with open('params/{}_params.txt'.format(Model_Select_Config.predict_target), "r") as f:
            params = f.read()
        best_params = eval(params)

         # 测试集上迭代7次
        test_mape, test_mape_result, test_results, feature_importance_df, model, train_results = \
                                                    model_evaluate(handled_data, params=best_params, test_rounds=5, test_period=3)
        print("15天交叉验证后的泛化误差为：{}".format(str(test_mape)))
        print("最佳参数为：{}".format(str(best_params)))   # 最佳参数
        index = [ '8.5-8.7', '8.8-8.10', '8.11-8.13', '8.14-8.16','8.17-8.19']

        if not os.path.exists('{}_results/'.format(str(Model_Select_Config.outfile_path))):  # 为本次结果建一个文件夹
            os.mkdir('{}_results'.format(str(Model_Select_Config.outfile_path)))
        output_path = '{}_results'.format(str(Model_Select_Config.outfile_path))  # 文件路径

        pd.Series(test_mape_result, index=index, name=Config.metric[0]).to_csv('{}/{}_{}_Mape_test_result_opti.csv'
            .format(output_path, date, Model_Select_Config.predict_target))                      # 7轮迭代的mape
        train_results.to_csv('{}/{}_{}_train_results_opti.csv'
            .format(output_path, date, Model_Select_Config.predict_target), index=False)         # 最后一轮迭代的整体预测结果
        test_results.to_csv('{}/{}_{}_test_results_opti.csv'
            .format(output_path, date, Model_Select_Config.predict_target), index=False)         # 7轮迭代的预测值
        feature_importance_df.to_csv('{}/{}_{}_feature_importance_opti.csv'
            .format(output_path, date, Model_Select_Config.predict_target), index=True)          # 7轮迭代的feature_importance
        # bayes_process_df.to_csv('{}/{}_{}_bayes_process_opti.csv'
        #     .format(output_path, date, Model_Select_Config.predict_target), index=False)             # 参数优化的过程记录










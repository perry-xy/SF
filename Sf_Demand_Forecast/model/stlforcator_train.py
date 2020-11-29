#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
# @Time    : 2019/10/30 16:06
# @Author  : peng.wang
# @Email   : WangPeng4@sfmail.sf-express.com
# @FileName: stlforcator_train.py
# @ProjectName :sh-demand-forecast-alg
"""
import os
import pandas as pd
import numpy as np
import datetime
import warnings
from monthdelta import monthdelta
from sklearn.pipeline import Pipeline
#
from utils.misc import Logger, map_forecast_periods
from config.config import EXTERNAL_COLUMNS
from core.metrics import mean_abs_percentage_error
from core.data_engineer import DataClean, DataMerge, DataScaler
from core.feature_engineer import FeatureEngineer, FeaturePostprocessing, FeatureSelector
from core.model import TrainModel
from utils import util

warnings.filterwarnings("ignore")
log = Logger(log_path=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'log')).logger


class STLForecastorTrain(object):
    def __init__(self, logger=log):
        self.data_interval = None
        self.data_month = None
        self.data_columns = None
        self.data_rows = None
        self.data_state = True
        self.with_external = False
        self.external_df = pd.DataFrame()
        self.external_column = []
        self.df = pd.DataFrame()
        self.forecast_period = "1month"
        self.forecast_mode = 'M1'
        self.ensemble = True
        self.feature = True
        self.param_optimization = None
        self.feature_selection = False
        self.predict_result = pd.DataFrame()
        self.save_model = False
        self.model_path = ""
        self.model_name = "XGB"
        self.key_columns = None
        self.id_column = 'id'
        self.target_column = 'target'
        self.time_column = 'date_time'
        self.predict_model_path = ''
        self.logger = logger
        self.save_feature = True

    def model_setup(self, model_name="XGB", param_optimization=False, save_model=True, model_path='model_file',
                    feature_selection=False):
        self.param_optimization = param_optimization
        self.model_name = model_name
        self.save_model = save_model
        self.model_path = os.path.join(os.getcwd(), model_path)
        self.feature_selection = feature_selection

        if save_model:
            if not os.path.exists(self.model_path):
                os.makedirs(self.model_path)
            cur_time = datetime.datetime.now().strftime("%Y%m%d")
            model_name = "forecast_model_single_{}_{}.pkl".format(model_name, cur_time)
            # model_name = "forecast_model_single_{}.pkl".format(model_name)
            self.model_path = os.path.join(model_path, model_name)

    def data_setup(self, df, target_column='quantity', id_column='id', time_column='date_time', with_external=False,
                   data_interval=1, forecast_period="1month", forecast_mode='M1', external_df=pd.DataFrame(),
                   external_column=EXTERNAL_COLUMNS):
        self.df = df
        self.data_interval = int(data_interval) #数据是以1天为间隔的，
        self.target_column = target_column
        self.id_column = id_column
        self.time_column = time_column
        self.key_columns = [self.id_column, self.time_column] #keyColumn是以city、time为关键列的
        self.forecast_period = forecast_period #周度预测
        self.forecast_mode = forecast_mode
        self.with_external = with_external
        if with_external:
            self.external_df = external_df
            self.external_column = external_column

    def build_model(self, save_feature=False, **cv_setting):
        if self.logger:
            self.logger.info('Forecastor: Start Checking data')
        # data length is less than 2 month or missing rate is more than 0.1, then training process being stopped
        self.data_checking() #检查数据是否足够做预测
        if self.data_state is False:
            if self.logger:
                self.logger.error('Forecaste: Input error')
            return pd.DataFrame()

        if self.logger:
            self.logger.info('Forecastor: start build the model')
        # TODO: when data is less than 0.5 month, maybe simple algorithm is a good choice such as MA, shift method
        # if self.data_month < 3:
        #     self.ensemble = False
        #     self.model_name = "GBDT"
        # if self.data_month < 0.5:
        #     self.ensemble = False
        #     self.feature = False
        #     self.model_name = "shift_method"

        if self.param_optimization:
            # choose the best parameters
            # step 1: do cross validation to choose the best model
            # step 2: training based on the best model
            # now the efficiency of parameters optimization is low, should improve in other method
            if self.logger:
                self.logger.info('Perform parameter optimization on model {}'.format(self.model_name))
            all_param_combination = TrainModel.build_parameter_combination(self.model_name)
            cv_result = []
            for n, cur_param in enumerate(all_param_combination):
                self.logger.info(cur_param)
                cur_result = self.model_cv(self.model_name, cur_param, save_feature=save_feature)
                cur_cv = {"parameter": cur_param, "result": cur_result}
                cv_result.append(cur_cv)
            best_param = TrainModel.model_selection(cv_result)
        else:
            # no need to choose the best parameters, but still doing a simple cross-validation,
            # get the multiple results to validate the prediction accuracy stability
            if self.logger:
                self.logger.info("Validate based on the cross-validation settings.") #根据交叉验证设置进行验证
            best_param = None

            if 'horizon' in cv_setting:
                horizon = cv_setting['horizon'] #7天
                period = cv_setting['period'] #7天
                train_end_date = cv_setting['train_end_date'] #2019.06.01
            else:
                horizon = '7days'
                period = '7days'
                train_end_date = '2019-06-01'
            _ = self.cross_validation(self.df, self.model_name, best_param, horizon, period, train_end_date)
            # _ = self.model_cv(self.model_name, best_param, one_step=True, save_feature=save_feature)

        if best_param is None:
            param_str = "default"
        else:
            param_str = "best"
        if self.logger:
            self.logger.info("Start training on model {} with {} parameters".format(self.model_name, param_str))
            self.logger.info(best_param)

        # train the model based on all the data
        trained_pipeline = Pipeline(self.construct_train_pipeline(self.model_name, best_param))
        model_dict = trained_pipeline.transform(self.df)
        return model_dict

    @staticmethod
    def save_feature_to_file(data_dict, object_id, save_index=None, actual_target=None):
        feature_df = data_dict["feature"]
        if save_index is None:
            save_index = feature_df.index

        feature_df = feature_df.loc[save_index]
        # feature_df["time"] = data_dict["time"][save_index]
        feature_df["target"] = data_dict["target"][save_index]
        if actual_target is None:
            feature_df.to_csv("{}_train_feature.csv".format(object_id), index=False)
        else:
            feature_df["target"] = actual_target
            feature_df.to_csv("{}_test_feature.csv".format(object_id), index=False)

    def model_cv_single_step(self, model_name, model_param, train_index, valid_index, save_feature=False):
        if self.logger:
            self.logger.info(
                'Validate model {} based on {} to {}'.format(model_name, valid_index[0], valid_index[-1]))
        train_data = self.df.loc[train_index]
        trained_pipeline = Pipeline(self.construct_train_pipeline(model_name, model_param))
        train_result_dict = trained_pipeline.transform(train_data)
        if save_feature:
            self.save_feature_to_file(train_result_dict, self.df.loc[0, self.id_column])

        # for validation only
        valid_df = self.df.copy()
        valid_target = valid_df.loc[valid_index, self.target_column].values

        valid_df.loc[valid_index, self.target_column] = 0
        feature_pipeline = Pipeline(self.construct_feature_pipeline("predict", train_result_dict['selector']))
        test_data_dict = feature_pipeline.transform(valid_df)
        # to avoid the feature column unmatch
        pred_array = TrainModel.predict_by_model(train_result_dict["model"],
                                                   test_data_dict["feature"].loc[valid_index],
                                                   train_result_dict["feature"].columns.values.tolist())
        pred_array = np.asarray(pred_array)
        if self.logger:
            self.logger.info('Validation error is {}'.format(mean_abs_percentage_error(valid_target, pred_array)))
        valid_result = pd.DataFrame(
            {"valid_index": valid_index, "date_time": valid_df.loc[valid_index, self.time_column],
             "true": valid_target, "predict": pred_array})
        if self.logger:
            self.logger.info(valid_result.to_csv(index=False))

        return valid_result

    def model_cv(self, model_name, model_param, one_step=True, save_feature=False):
        # if one_step is true, we only validate based on last month's data.
        forecast_days = map_forecast_periods(self.forecast_period)
        cv_index_list = TrainModel.split_train_validate(self.df[self.time_column], forecast_days)
        valid_result = pd.DataFrame()
        if one_step:
            cv_index_list = [cv_index_list[-1]]
        result_list = []
        for n, index_dict in enumerate(cv_index_list):
            if self.logger:
                self.logger.info("Modeling cv on the batch {}".format(n))
            valid_index = index_dict["valid"]
            valid_result = self.model_cv_single_step(model_name, model_param, index_dict["train"],
                                                     index_dict["valid"], save_feature=save_feature)
            valid_result[self.time_column] = self.df.loc[valid_index, self.time_column].values
            result_list.append(valid_result)
        result_df = pd.concat(result_list).reset_index(drop=True)
        if self.logger:
            self.logger.info('Validation error is {}'.format(mean_abs_percentage_error(result_df["true"],
                                                                                       result_df["predict"])))
        return valid_result

    def construct_feature_pipeline(self, input_feature_type="train", selector=None):
        pipeline_list = []

        # dataformat includes:data clean, data merge, data interpolation, data sampling and data scaling
        dataclean_ins = DataClean(time_series=True,
                                  time_column=self.time_column,
                                  target_column=self.target_column,
                                  logger=self.logger)
        pipeline_list.append(("DataClean", dataclean_ins))
        if self.with_external:
            datamerge_external_ins = DataMerge(self.external_df, self.time_column, logger=self.logger)
            pipeline_list.append(("DataMerge_external_data", datamerge_external_ins))
        # TODO: doing scaling

        # featureengineering includes: features generation, postprocessing and selection
        if self.feature:
            featureeng_ins = FeatureEngineer(forecast_period=self.forecast_period,
                                             data_interval=self.data_interval,
                                             time_column=self.time_column,
                                             target_column=self.target_column,
                                             id_column=self.id_column,
                                             logger=self.logger)
            pipeline_list.append(("FeatureEngineering", featureeng_ins))

            featurepost_ins = FeaturePostprocessing(input_feature_type=input_feature_type,
                                                    target_column=self.target_column,
                                                    time_column=self.time_column,
                                                    key_columns=self.key_columns,
                                                    logger=self.logger)
            pipeline_list.append(("FeaturePostprocessing", featurepost_ins))

            # feature format, including: feature selection, feature optimize, feature normalize
            if self.feature_selection:
                featureselect_ins = FeatureSelector(selector, input_feature_type, logger=self.logger)
                pipeline_list.append(("FeatureSelection", featureselect_ins))
        return pipeline_list

    def construct_train_pipeline(self, model_name, model_param=None):

        train_pipeline = self.construct_feature_pipeline(input_feature_type="train")

        # model train, including: model param optimization, train model
        trainmodel_ins = TrainModel(model_name=model_name,
                                    model_param=model_param,
                                    param_optimize=self.param_optimization,
                                    key_columns=self.key_columns,
                                    target_column=self.target_column,
                                    save_model=self.save_model,
                                    model_path=self.model_path,
                                    data_interval=self.data_interval,
                                    logger=self.logger)
        train_pipeline.append(("TrainModel", trainmodel_ins))
        return train_pipeline

    # the data checking before train the model, the target missing rate should more than specified ratio
    # the data length should more than 2 month
    # TODO: disable this function right now
    def data_checking(self, minimum_month=2, target_missing_rate=0.1):
        """
        :param minimum_month: the unit is in terms of month
        :param target_missing_rate
        :return:
        """
        try:
            self.data_columns = self.df.shape[1]
            self.data_rows = self.df.shape[0]
            self.df.date_time = pd.to_datetime(self.df[self.time_column])
            # resample the data in order to confirm everyday has the data
            # self.df = self.df.resample('D').sum().fillna(0)
            total_minutes = (self.df.date_time.max() - self.df.date_time.min()).total_seconds() / 60.0
            total_days = (self.df.date_time.max() - self.df.date_time.min()).total_seconds() / 60.0/60.0/24.0
            self.data_month = total_minutes / (60 * 24 * 30.5)
            minimum_rows = total_days / self.data_interval * 0.5
            if self.data_rows < minimum_rows or self.data_month < minimum_month:
                if self.logger:
                    self.logger.error("Input data is not enough for training a model!")
                self.data_state = False
            else:
                self.df.drop_duplicates(subset=self.key_columns, inplace=True)
                self.df.sort_values(by=self.time_column, inplace=True)
                self.df.reset_index(drop=True, inplace=True)

            try:
                self.df[self.target_column] = self.df[self.target_column].apply(lambda x: float(x))
            except:
                if self.logger:
                    self.logger.error('DataFormat: key column type error!')

            target_missing_index = self.df[(self.df[self.target_column].isnull())].index.tolist()
            missing_rate = len(target_missing_index) / self.data_rows
            if self.logger:
                self.logger.warning("Target column missing rate is {}.".format(missing_rate))
            if missing_rate > target_missing_rate:
                self.data_state = False

        except Exception as e:
            self.logger.error('Input Check: date time error!' + str(e))

    def cross_validation(self, df, model_name, model_param, horizon, period,
                         train_end_date, test_end_date=None, forecast_mode=1):
        """
        Cross-Validation for time series.
        Computes forecasts from historical cutoff points. Beginning from
        (end - horizon), works backwards making cutoffs with a spacing of period
        until initial is reached.
        :param df:
        :param model_name:
        :param model_param
        :param forecast_mode:
        :param horizon:
        :param train_end_date:
        :param test_end_date:
        :param period:
        :return:
        """

        horizon = pd.Timedelta(horizon) #7天
        period = pd.Timedelta(period) #7天
        train_end_date = pd.to_datetime(train_end_date) #2019.06.01

        cutoffs = util.generate_cutoffs(df, self.time_column, horizon, period=period, train_end_date=train_end_date)
        for cutoff in cutoffs:
            if self.logger:
                self.logger.info('validate data between {} and {}'.format(cutoff, cutoff+forecast_mode*period))
            train_data = df[df[self.time_column] <= cutoff] #取验证集之前的数据
            print("train_data"+str(len(train_data)))
            trained_pipeline = Pipeline(self.construct_train_pipeline(model_name, model_param))
            train_result_dict = trained_pipeline.transform(train_data)
            if self.feature_selection:
                train_feature_selector = train_result_dict['selector']
            else:
                train_feature_selector = None
                train_feature_df = train_result_dict["feature"]
            # get the validation data
            valid_index = (df[self.time_column] > cutoff+(forecast_mode-1)*horizon) & \
                          (df[self.time_column] <= cutoff + forecast_mode*horizon)
            validation_data = df[df[self.time_column] <= cutoff + forecast_mode*horizon]
            valid_target = df[self.target_column].loc[valid_index].values
            validation_data.loc[valid_index, self.target_column] = 0
            feature_pipeline = Pipeline(self.construct_feature_pipeline(input_feature_type="predict",
                                                                        selector=train_feature_selector))
            test_data_dict = feature_pipeline.transform(validation_data)
            # to avoid the feature column unmatch
            # TODO:
            pred_array = TrainModel.predict_by_model(train_result_dict["model"],
                                                     test_data_dict["feature"].loc[valid_index],
                                                     test_data_dict["feature"].columns.values.tolist())
            if self.logger:
                self.logger.info('Validation error is {}'.format(mean_abs_percentage_error(valid_target, pred_array)))
            valid_result = pd.DataFrame(
                {self.time_column: validation_data.loc[valid_index, self.time_column],
                 "actual": valid_target, "predict": pred_array})
            valid_result['mape'] = mean_abs_percentage_error(valid_target, pred_array)
            if True:
                valid_result.to_csv('validation/'+str(cutoff)[0:10] +'.csv')
        return valid_result




#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
# @Time    : 2019/11/12 21:42
# @Author  : peng.wang
# @Email   : WangPeng4@sfmail.sf-express.com
# @FileName: stlforecastor_predict.py
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


class STLForecastorPredict(object):
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
            # model_name = "forecast_model_single_{}_{}.pkl".format(model_name, cur_time)
            model_name = "forecast_model_single_{}.pkl".format(model_name)
            self.model_path = os.path.join(model_path, model_name)

    def data_setup(self, df, target_column, id_column='id', time_column='date_time', with_external=False,
                   data_interval=1, forecast_period="1month", forecast_mode='M1', external_df=pd.DataFrame(),
                   external_column=EXTERNAL_COLUMNS):
        self.df = df
        self.data_interval = int(data_interval)
        self.target_column = target_column
        self.id_column = id_column
        self.time_column = time_column
        self.key_columns = [self.id_column, self.time_column]
        self.forecast_period = forecast_period
        self.forecast_mode = forecast_mode
        self.with_external = with_external
        if with_external:
            self.external_df = external_df
            self.external_column = external_column

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
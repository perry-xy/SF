#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
# @Time    : 2019/9/30 17:44
# @Author  : peng.wang
# @Email   : WangPeng4@sfmail.sf-express.com
# @FileName: compat.py
# @ProjectName :sh-demand-forecast-alg
"""

# sklearn
try:
    from sklearn.base import BaseEstimator
    from sklearn.base import RegressorMixin, ClassifierMixin
    from sklearn.preprocessing import LabelEncoder

    try:
        from sklearn.model_selection import KFold, StratifiedKFold
    except ImportError:
        from sklearn.cross_validation import KFold, StratifiedKFold

    SKLEARN_INSTALLED = True

    XGBModelBase = BaseEstimator
    XGBRegressorBase = RegressorMixin

    XGBKFold = KFold
    XGBStratifiedKFold = StratifiedKFold
    XGBLabelEncoder = LabelEncoder
except ImportError:
    SKLEARN_INSTALLED = False

    # used for compatibility without sklearn
    XGBModelBase = object
    XGBRegressorBase = object

    XGBKFold = None
    XGBStratifiedKFold = None
    XGBLabelEncoder = None

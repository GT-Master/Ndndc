# -*- coding: utf-8 -*-
"""
Created on Fri Aug 11 15:46:09 2017

Responsability:
Custom transformer to add exponential average columns to a dataset

@author: ruiz-i
"""
from sklearn.base import TransformerMixin, BaseEstimator
import numpy as np
import pandas as pd

class ExponentialAverage(BaseEstimator, TransformerMixin):
    
    def __init__(self, number=7, ow=False):
        self._number = number
        self._ow = ow
        
    def fit(self, X, y=None):
        return self
        
    def transform(self, X, y=None):
        Xmv = X.apply(self._exp_average, 0)
        if (self._ow):
            result = Xmv
        else:
            result = pd.concat([X,Xmv], axis=1, join='inner')
        
        return result

    def _exp_average(self, serie):
        result = serie.ewm(ignore_na=False,min_periods=1,adjust=True,com=self._number).mean()
        return result
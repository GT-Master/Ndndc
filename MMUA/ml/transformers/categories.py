# -*- coding: utf-8 -*-
"""
Created on Fri Aug 11 11:44:17 2017

Responsability:
Custom Transformer for categorize continuous data

@author: ruiz-i
"""
from sklearn.base import TransformerMixin, BaseEstimator

class Categories(BaseEstimator, TransformerMixin):
    
    def __init__(self, number = 10):
        self._number = number

    def fit(self, X, y=None):
        return self
        
    def transform(self, X, y=None):
        '''Normalize the serie values and divide them in ten categories
        
        Arguments
        - Serie: the values to normalize
        - n: 1 for ten categories, 2 for five, 3 for three
        '''
        will 
        maxval = serie.max()
        minval = serie.min()
        #serie = (((serie-minval)/(maxval-minval))/n).round(1)
        serie = ((serie-minval)/(maxval-minval)).round(1)
        
        return serie
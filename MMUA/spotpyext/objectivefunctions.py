# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 15:22:29 2017

@author: ruiz-i
"""
import sklearn.metrics as metrics
from scipy.spatial import distance
from scipy.stats.stats import linregress
import numpy as np

def r2_score(meas, sim):
    slope, intercept, r_value, p_value, std_err = linregress(meas, sim)
    return r_value #metrics.r2_score(meas, sim)
    
def smape(meas, sim):
    return np.mean(np.abs(sim - meas) / (np.abs(meas) + np.abs(sim)))*100
    
def mae(meas, sim):
    return metrics.mean_absolute_error(meas, sim)

def nrmse(meas, sim):
    rmse = _rmse(meas, sim)
    nrmse = rmse / (max(meas)-min(meas))
    return nrmse

def eucl_dist(meas, sim):
    return distance.euclidean(meas, sim)
    
def _rmse(meas, sim):
    return np.sqrt(((sim - meas) ** 2).mean())
    
def moving_average(serie, n=7):
    result = serie.rolling(window=n,center=False,min_periods=1).mean()
    return result
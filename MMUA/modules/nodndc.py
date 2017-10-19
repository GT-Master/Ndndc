# -*- coding: utf-8 -*-
"""
Created on 
Responsability
Prepare Nodndc model

@author: ruiz-i
"""
import os
from datetime import datetime
import time
import sys
import logging
import numpy as np
import pandas as pd
import scipy as sci
import ldndc.measurement as meas

import ldndc.config as config

from ldndc.inputs import ProjectFile

class Nodndc(object):
    
    def __init__(self):
                        
        self.owd = os.getcwd()
              
        logging.debug('Ini Nodndc Setup')
        
        self.__model_inputs_path = config.parser.get('global', 'modelinputspath')
        self.__rollwindow = 7
        site_cfgs = str.split(config.parser.get('global', 'sites'),"|")
        self.__features = config.parser.items("features")
        self.__sites = []
        for site_key in site_cfgs:
            cfg = {}
            cfg['datastart'] = datetime.strptime(config.parser.get(site_key, 'datastart'), '%Y-%m-%d')  # Comparision with Meas data starts
            cfg['dataend'] = datetime.strptime(config.parser.get(site_key, 'dataend'), '%Y-%m-%d')  # Model and comparision stops
            cfg['measurementspath'] = config.parser.get(site_key, 'measurementspath')

            cfg['ldndcfile'] = config.parser.get(site_key, 'ldndcfile')
            cfg['measconf'] = config.parser.items("measurements_"+site_key)
            self.__sites.append(cfg)

        datasets = []
        self.__dataset = None
        for cfg in self.__sites:
            data = self.get_dataset(cfg)
            datasets.append(data)
        
        self.__dataset = self._merge(datasets)

    @property    
    def target(self):
        return pd.DataFrame(self.__dataset['n_n2o'])
        
    @property    
    def data(self):
        return self.__dataset.drop(['n_n2o', 'n_n2o_std'], axis=1)
        

    def _get_meas_data(self, cfg, rollwindow=7):
        ''' Read the measurement files '''
        meas_roll = []
        meas_loader = meas.Measurement(cfg['measurementspath'], cfg['datastart'], cfg['datastart'], cfg['dataend'])
        for name, meas_conf in cfg['measconf']:
            conf = str.split(meas_conf,"|")
            meas_roll.append(meas_loader.read_and_rolling(measname=name, measfile=conf[0],
                                              multiplier=float(conf[1]), window=rollwindow))
        return meas_roll

    def _get_input_data(self, cfg, rollwindow=7):
        inputs = ProjectFile(self.__model_inputs_path, cfg['ldndcfile'])
        inputs_d = inputs.load_dataframe()
        valid_cols = []
        for name, value in self.__features:
            valid_cols.append(name)
            lfun = self._getfun(value)
            inputs_d[name] = lfun(inputs_d[name])
       
        return inputs_d[valid_cols]
    
    def get_dataset(self, cfg):
        meas_data = self._get_meas_data(cfg, rollwindow=self.__rollwindow)
        input_d = self._get_input_data(cfg)
        for meas_v in meas_data:
            input_d = input_d.join(meas_v.ix[:,0], how ='inner')
            input_d = input_d.join(meas_v.ix[:,1], how ='left')
        return input_d

    def _merge(self, datasets):
        ''' Creates one unique dataframe'''
        merged = None
        for df in datasets:
            if merged is None:
                new_index = range(len(df))
                df['i'] = new_index
                df = df.set_index('i')
                merged = df
            else:
                new_index = range(len(merged),len(merged)+len(df))
                df['i'] = new_index
                df = df.set_index('i')
                merged = merged.merge(df, how='outer')
            
        return merged
        
    def _getfun(self, fun):
        spls = str.split(fun,"|")
        fname = spls[0]
        if fname == 'expavg':
            return lambda serie: self._exp_average(serie,int(spls[1]))
        elif fname == 'mavg':
            return lambda serie: self._moving_average(serie,int(spls[1]))
        else: #No known function.
            return lambda serie: serie
    
    def _moving_average(self,serie, n=7):
        ''' Data like precipitation or temperature behaves better with moving averages'''
        result = serie.rolling(window=n,center=False,min_periods=1).mean()
        return result
        
    def _exp_average(self, serie, n=7):
        ''' Data like management events are better exploited with exponential moving averages'''
        result = serie.ewm(ignore_na=False,min_periods=1,adjust=True,com=n).mean()
        return result
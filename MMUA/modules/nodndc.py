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
        site_cfgs = config.parser.items('global', 'sites')
        self.__sites = []
        for site_key in site_cfgs:
            cfg = {}
            cfg['datastart'] = datetime.strptime(config.parser.get(site_key, 'datastart'), '%Y-%m-%d')  # Comparision with Meas data starts
            cfg['dataend'] = datetime.strptime(config.parser.get(site_key, 'dataend'), '%Y-%m-%d')  # Model and comparision stops
            cfg['meas_path'] = config.parser.get(site_key, 'measurementspath')

            cfg['site_path'] = config.parser.get(site_key, 'ldndcfile')
            cfg['measconf'] = config.parser.items("measurements_"+site_key)
            self.__sites.append(cfg)

        datasets = []
        self.__dataset = None
        for cfg in self.__sites:
            data = self.get_dataset(cfg)
            data = self._transform(data)
            datasets.append(data)
            self.__dataset = self._merge(datasets)
        

    def _get_meas_data(self, cfg, rollwindow=7):
        ''' Read the measurement files '''
        meas_roll = []
        meas_loader = meas.Measurement(cfg.meas_path, cfg.datastart, cfg.datastart, cfg.dataend)
        for name, meas_conf in cfg.measconf:
            conf = str.split(meas_conf,"|")
            meas_roll.append(meas_loader.read_and_rolling(measname=name, measfile=conf[0],
                                              multiplier=float(conf[1]), window=rollwindow))
        return meas_roll

    def _get_input_data(self, cfg, rollwindow=7):
        inputs = ProjectFile(self.__model_inputs_path, cfg.site_path)
        inputs_d = inputs.load_dataframe()
        return inputs_d
    
    def get_dataset(self, cfg):
        meas_data = self._get_meas_data(cfg, rollwindow=self.__rollwindow)
        input_d = self._get_input_data(cfg)
        for meas_v in meas_data:
            input_d = input_d.join(meas_v.ix[:,0], how ='inner')
            input_d = input_d.join(meas_v.ix[:,1], how ='left')
        return input_d

    def _transform(self, data):
        ''' Clean and transform the data'''
        #TODO: 
        return data
    
    def _merge(self, datasets):
        ''' Creates one unique dataframe'''
        #TODO:
        return datasets
    
    def _moving_average(self,serie, n=7):
        ''' Data like precipitation or temperature behaves better with moving averages'''
        result = serie.rolling(window=n,center=False,min_periods=1).mean()
        return result
        
    def exp_average(self, serie, n=7):
        ''' Data like management events are better exploited with exponential moving averages'''
        result = serie.ewm(ignore_na=False,min_periods=1,adjust=True,com=n).mean()
        return result
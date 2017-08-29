# -*- coding: utf-8 -*-
"""
Created on May 2016

Responsability:
Read and prepare measurement files and output files for model execution and evaluation

@author: ignacio santabarbara, based in Tobias Houska garmisch version
"""
import os
import time
from datetime import datetime
import pandas as pd
import logging

class Measurement(object):

    def __init__(self,meas_path,modelstart,datastart,dataend):
        self.__model_start= modelstart.date()
        self.__data_start   = datastart.date()
        self.__data_end     = dataend.date()
        self.__working_dir = os.getcwd()
        self.__meas_path   = meas_path

    def read(self, measname, measfile, multiplier=1):
        '''loads measurements from file'''
        logging.debug('reading file: ' + measfile + ' from path:' + self.__meas_path)
        os.chdir(self.__meas_path)
        date2ts = lambda x: int(time.mktime(datetime.strptime(x, '%Y-%m-%d').timetuple()))
        
        data = pd.read_csv(measfile, sep="\t", skiprows=self._get_data_line(measfile), usecols=[0,2,3],
                           converters={'date':date2ts}, index_col=['date'])
        
        start_ts = int(time.mktime(self.__data_start.timetuple()))
        end_ts = int(time.mktime(self.__data_end.timetuple()))
        data = data[(data.index >= start_ts) & (data.index <= end_ts)]
        data[data.columns.values[0]] = data[data.columns.values[0]]*multiplier
        data[data.columns.values[1]] = data[data.columns.values[1]]*multiplier
        os.chdir(self.__working_dir)
        return data
    
    def read_and_rolling(self, measname, measfile, multiplier=1, window=7):
        '''loads measurements from file and make a moving average'''
        logging.debug('reading file: ' + measfile + ' from path:' + self.__meas_path)
        os.chdir(self.__meas_path)
        date2ts = lambda x: int(time.mktime(x.timetuple()))
        date2dt = lambda x: datetime.strptime(x, '%Y-%m-%d')
        data = pd.read_csv(measfile, sep="\t", skiprows=self._get_data_line(measfile), usecols=[0,2,3],
                           converters={'date':date2dt}, index_col=['date'])
        
        data1 = data.resample('1D').ffill().rolling(window=window, min_periods=1).mean()
        data = data1[data1.index.isin(data.index)]
        data.index = data.index.map(date2ts)
        
        start_ts = int(time.mktime(self.__data_start.timetuple()))
        end_ts = int(time.mktime(self.__data_end.timetuple()))
        data = data[(data.index >= start_ts) & (data.index <= end_ts)]
        data[data.columns.values[0]] = data[data.columns.values[0]]*multiplier
        data[data.columns.values[1]] = data[data.columns.values[1]]*multiplier
        os.chdir(self.__working_dir)
        return data
        
    def _get_data_line(self, measfile):
        '''Get the first line number of the measurement data'''
        with open(measfile) as lines:
            for num, line in enumerate(lines, 1):
                if '%data' in line:
                    return num
        return 12


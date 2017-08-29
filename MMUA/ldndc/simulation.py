# -*- coding: utf-8 -*-
"""
Created on Wed Mar 22 12:04:30 2017

Responsability:
1.Ldndc simulation output

@author: ruiz-i
"""

import os
import glob
import sys
import pandas as pd
import time
from datetime import datetime
import logging

class Simulation(object):

    def __init__(self, model_input_path):
            self.__model_input_path = model_input_path
            self.__prefix_path = None
            self.__owd = os.getcwd() #checks out your working directory
            self.__data = {}
            
    def read(self, simulations, sinkprefix, call=None):
        os.chdir(self.__model_input_path)
        self.__prefix_path = os.path.normpath(self.__model_input_path + os.sep + sinkprefix)
        try:
            for colname, prefix in simulations:
                
                layer = None
                if prefix.endswith("layer"):
                    prfxs = prefix.split("|")
                    prefix = str(call) + "_" + prfxs[0]
                    layer = int(prfxs[1])
                    layercol = ["layer"]
                else:
                    layercol = []
                    prefix = str(call) + "_" + prefix
                    
                colnames = colname.split("+")
                cols = ['datetime']+colnames+layercol
                path = self.__prefix_path + prefix
                date2ts = lambda x: int(time.mktime(datetime.strptime(x, '%Y-%m-%d %H:%M:%S').timetuple()))
                df = pd.read_csv(path, sep="\t", usecols=cols, 
                                 converters={'datetime':date2ts}, index_col=['datetime'])
                #Filter the layer
                if layer is not None:
                    df = df[(df['layer'] >= layer)]
                    del df['layer']
                    df = df.groupby([df.index]).sum()

                #Group columns
                if len(colnames) > 1:
                    df = df.sum(axis=1).to_frame()
                    df.columns = [colname]
                self.__data[colname] = df
        finally:
            os.chdir(self.__owd)
            self._remove_files(call)
            
        
    def _remove_files(self, call):
        os.chdir(self.__model_input_path)
        try:
            #TODO: it raise an Exception, but the file are deleted. Understand
            map(os.remove, glob.glob(self.__prefix_path + str(call) + "*.txt"))
            map(os.remove, glob.glob(self.__prefix_path + str(call) + "*.log"))

        except OSError:
            logging.error("Cannot remove files for:" + self.__prefix_path)
            logging.error(str(sys.exc_info()[1]))

        os.chdir(self.__owd)

    def getsim(self, colname=None):
        '''Return sim data fot the column'''
        
        if colname is None:
            raise Exception('Cannot get sim. Missing colname')
        
        sim_data = self.__data.get(colname)
        return sim_data[colname]

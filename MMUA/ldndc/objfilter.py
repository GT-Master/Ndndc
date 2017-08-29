# -*- coding: utf-8 -*-
"""
Created on Tue May 09 10:43:41 2017
Responsability: prepare the likelihood filters

@author: ruiz-i
"""

import numpy as np
import logging
#import ldndc.config as config
#
#
# Example of filters:
# - With function
# pnrmse0=<=|0.1|percentile
# - Without function
# nrmse0=<=|0.003
#
class Filter(object):
    def __init__(self, filters):

        self.objs = []
        self.operators = []
        self.limits = []
                         
        for likname, comp in filters:
            self.objs.append(likname)
            spls = comp.split('|')
            self.limits.append(spls[0])
            if len(spls) > 1:
                self.operators.append(spls[1])
            else:
                self.operators.append("<=")
                
            #self.thfuns.append(self._getfun(spls))
        
    def _getfun(self, spls):
        '''DEPRECATED'''
        if len(spls) == 2:
            return lambda liks: float(spls[1])
        else:
            fname = spls[2]
            if fname == 'percentile':
                return lambda liks: np.percentile(liks,float(spls[1]))
            else: #No known function.
                logging.error("Function " +  fname + " is not configured in filter options. Basic threshold will be set")
                return lambda liks: float(spls[1])
            
            
    
    
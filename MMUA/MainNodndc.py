# -*- coding: utf-8 -*-
"""
Created on Thu Aug 24 17:15:26 2017
Responsability:
This class is the main program to create a Nodndc model

@author: ruiz-i
"""


from ldndc.datasets import Datasets
import logging
import os
import sys
from sklearn import metrics

import ldndc.config as config
from ldndc.persistence import Persistence

from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_val_score, KFold
from sklearn import ensemble
from scipy.stats import sem
import numpy as np

from sklearn import svm
from sklearn import preprocessing
from modules import nodndc as model


if __name__ == "__main__":
        
    config_file = str(sys.argv[1])
    
    config.parser = config.set(config_file)
    logfile = config.parser.get('global', 'logfile')
    loglevel = config.parser.get('global', 'loglevel')
    logging.basicConfig(filename=logfile,level=loglevel)
    logging.debug("config file:"+ config_file)
    logging.info("Start creating the Nodndc!")
    
    ndndc = model.Nodndc()
    

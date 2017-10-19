# -*- coding: utf-8 -*-
"""
Created on Wed Aug 30 17:38:17 2017

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
import matplotlib.pyplot as plt

import matplotlib.pyplot as plt
import matplotlib.dates as md
import pandas as pd

import datetime as dt

if __name__ == "__main__":

    meas_path = r"C:\projects\measurements\arable\FR_grignon"
    datastart = datetime.strptime('2004-01-01', '%Y-%m-%d')
    dataend = datetime.strptime('2008-12-31', '%Y-%m-%d')
    measconf = {"n_n2o":"FR_grignon_N2O.txt|0.00024"}
    
    meas_path = r"C:\projects\measurements\arable\DE_paulinenaue"
    datastart = datetime.strptime('2007-01-01', '%Y-%m-%d')
    dataend = datetime.strptime('2009-12-31', '%Y-%m-%d')
    measconf = {"n_n2o":"DE_paulinenaue_N2O.txt|0.001"}
    


    def _get_meas_data(rollwindow=7):
        ''' Read the measurement files '''
        meas_roll = []
        meas_loader = meas.Measurement(meas_path,datastart, datastart, dataend)
        for name, meas_conf in measconf.iteritems():
            conf = str.split(meas_conf,"|")
            meas_roll.append(meas_loader.read_and_rolling(measname=name, measfile=conf[0],
                                              multiplier=float(conf[1]), window=rollwindow))
        return meas_roll
        
    data = _get_meas_data()[0]
    dates=[dt.datetime.fromtimestamp(ts) for ts in data.index.values]
    datenums=md.date2num(dates)
    
    plt.figure(1)
    plt.subplot(1,1,1)
    plt.tight_layout()
   
    plt.plot(datenums, data['n_n2o'], alpha=1, zorder=1)

    plt.show()



# -*- coding: utf-8 -*-
"""
Created on Mon Apr 10 15:58:01 2017

@author: ruiz-i
"""
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as md
import pandas as pd
from persistence import Persistence
import datetime as dt
import time
import os

class Plot(object):
    def __init__(self, db, obj_filter):
        self.__filter = obj_filter
        self.__db = db
    
    def _normalize(self,values, maxval, minval):
        return (values-minval)/(maxval-minval)
    
    def normtest(self):
        self.__db.normalize_likelihood_table()
    
    def bestResults(self, n, comp, meas=None):
        id_comp = self.__db.get_name_ids([comp])
        id_meas = self.__db.get_name_ids([meas])
        
        for i in range(0,n):
            iteration = self.__db.get_best_normalized_iteration(self.__filter.objs,self.__filter.limits,self.__filter.operators, index=i)
            output = self.__db.get_output(id_comp=id_comp[0], iteration=iteration)
        
    def plotBestOutput(self, comp, meas=None):
        id_comp = self.__db.get_name_ids([comp])
        id_meas = self.__db.get_name_ids([meas])
        #TODO: workaround
        iteration = 96938#9921#15472#65556#35690#497#96938#self.__db.get_best_normalized_iteration(self.__filter.objs,self.__filter.limits,self.__filter.operators)
        output = self.__db.get_output(id_comp=id_comp[0], iteration=iteration)
        
        if meas is not None:
            meas = self.__db.get_measurement(id_name=id_meas[0])
            datesmeas=[dt.datetime.fromtimestamp(ts) for ts in meas.index.values]
            datemeasnums=md.date2num(datesmeas)
        
        dates=[dt.datetime.fromtimestamp(ts) for ts in output.index.values]
        datenums=md.date2num(dates)
        plt.figure(1)
        plt.subplot(1,1,1)
        plt.tight_layout()
        plt.scatter(datemeasnums, meas['value'], marker='o', color='r',alpha=0.1, zorder=2)
        plt.title(comp)
        plt.plot(datenums, output['value'], alpha=1, zorder=1)
  
        plt.savefig('foo.png', bbox_inches='tight')
        plt.show()
        
    def plotMLOutputs(self, comp, targets):
        id_comp = self.__db.get_name_ids([comp])
       # id_meas = self.__db.get_name_ids([meas])
        
        iteration = self.__db.get_best_normalized_iteration(self.__filter.objs,self.__filter.limits,self.__filter.operators)
        output = self.__db.get_output(id_comp=id_comp[0], iteration=iteration)
        
        dates=[dt.datetime.fromtimestamp(ts) for ts in output.index.values]
        datenums=md.date2num(dates)
        plt.figure(1)
        plt.subplot(1,1,1)
        plt.tight_layout()
        #plt.scatter(datemeasnums, meas['value'], marker='o', color='r',alpha=0.1, zorder=2)
        plt.title(comp)
        plt.plot(datenums, output['value'], alpha=1, zorder=1)
        high = output['value']+targets[:,0]
        low = output['value']+targets[:,1]
        plt.plot(datenums, high, alpha=1, zorder=1)
        plt.plot(datenums,low, alpha=1, zorder=1)
  
        plt.show()
        
    def plot1rtFilterOutput(self, comp, meas=None):
        id_comp = self.__db.get_name_ids([comp])
        id_meas = self.__db.get_name_ids([meas])
        id_objs = self.__db.get_name_ids(self.__filter.objs)
        
        output = self.__db.get_output_join_likelihood(id_comp=id_comp[0], id_obj=id_objs[0], limit=self.__filter.limits[0], operator=self.__filter.operators[0])
        
        if meas is not None:
            meas = self.__db.get_measurement(id_name=id_meas[0])
            datesmeas=[dt.datetime.fromtimestamp(ts) for ts in meas.index.values]
            datemeasnums=md.date2num(datesmeas)
        
        dates=[dt.datetime.fromtimestamp(ts) for ts in output.index.values]
        datenums=md.date2num(dates)
        plt.figure(1)
        plt.subplot(1,1,1)
        plt.tight_layout()
        #plt.title(str(lik) + " len:" + str(len(np.unique(newout['iteration']))))
        #plt.ylim(minmeas,maxmeas)
        plt.scatter(datemeasnums, meas['value'], marker='o', color='r',alpha=0.1, zorder=2)
                
        plt.plot(datenums, output['value'], alpha=1, zorder=1)
                
                #i = i + 1
     
        plt.show()
        
    def plotOutput(self, comp, meas=None):
        id_comp = self.__db.get_name_ids([comp])
        id_meas = self.__db.get_name_ids([meas])
        
        output = self.__db.get_output(id_comp=id_comp[0])
        
        if meas is not None:
            meas = self.__db.get_measurement(id_name=id_meas[0])
            datesmeas=[dt.datetime.fromtimestamp(ts) for ts in meas.index.values]
            datemeasnums=md.date2num(datesmeas)
        
        dates=[dt.datetime.fromtimestamp(ts) for ts in output.index.values]
        datenums=md.date2num(dates)
        plt.figure(1)
        plt.subplot(1,1,1)
        plt.tight_layout()
        #plt.title(str(lik) + " len:" + str(len(np.unique(newout['iteration']))))
        #plt.ylim(minmeas,maxmeas)
        plt.scatter(datemeasnums, meas['value'], marker='o', color='r',alpha=0.1, zorder=2)
                
        plt.plot(datenums, output['value'], alpha=1, zorder=1)
                
                #i = i + 1
     
        plt.show()
        
       
        
    def plotFilteredGrid(self,comp, meas=None):
        id_comp = self.__db.get_name_ids([comp])
        id_meas = self.__db.get_name_ids([meas])
        id_objs = self.__db.get_name_ids(self.__filter.objs)

        outdf = self.__db.get_output_join_likelihoods(id_comp=id_comp[0], id_objs=id_objs, limits=self.__filter.limits, operators=self.__filter.operators)
        
        if meas is not None:
            meas = self.__db.get_measurement(id_name=id_meas[0])
            datesmeas=[dt.datetime.fromtimestamp(ts) for ts in meas.index.values]
            datemeasnums=md.date2num(datesmeas)
       
        outdf['likelihood'] = (outdf['likelihood'] - outdf['likelihood'].min()) / (outdf['likelihood'].max() - outdf['likelihood'].min())

        best = outdf['likelihood'].min()
        i = 1
        #TODO: make sure the order
        for rg in np.arange(1.1,0,-0.1):
            prevlik = rg
            lik = rg + 0.1
            if lik == 1.1:
                prevlik = best - 0.01
                lik = best

            newout = outdf.query('likelihood <= ' + str(lik) + ' & likelihood > ' + str(prevlik))
            dates=[dt.datetime.fromtimestamp(ts) for ts in newout.index.values]
            datenums=md.date2num(dates)
            plt.figure(1)
            #TODO: meas could be None
            minmeas =  meas['value'].min()
            maxmeas =  meas['value'].max()*3
            if len(newout) > 0:
                plt.subplot(5,3,i)
                plt.tight_layout()
                plt.title(str(lik) + " len:" + str(len(np.unique(newout['iteration']))))
                plt.ylim(minmeas,maxmeas)
                plt.scatter(datemeasnums, meas['value'], marker='o', color='r',alpha=0.05, zorder=2)
                
                plt.plot(datenums, newout['value'], alpha=1, zorder=1)
                
                i = i + 1
     
        plt.show()
    
    def plotTargetVsInputs(self, targets, inputs):
        '''
        
        '''
        pass

    
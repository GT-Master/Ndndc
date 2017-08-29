# -*- coding: utf-8 -*-
"""
Created on Wed Mar 22 12:06:39 2017

Responsability:
Prepare the Ldndc uncertainty datasets for sklearn

Dataset structure:

@author: ruiz-i
"""

import numpy as np


class Datasets(object):
    def __init__(self, db, target_comp, inputs, mov_avg=7):
        '''Prepare the access to Ldndc Uncertainty DB'''
        self.__db = db
        self.__target_comp = target_comp
        self.__inputs = inputs
        self.__mov_avg=mov_avg
        self.target = None
        self.data = None
        
    def load(self, training=True):
        self.data = self.load_data()
        if training:
            self.target = self.load_target()
        else:
            self.target = self.load_median()
        
    def load_target(self):
        #data = self._load_PI(alpha=0.95)
        #data = self._load_cv()
        data = self._load_std()
        #data = self._load_range()
        return data
    
    def load_median(self):
        id_comps = self.__db.get_name_ids(self.__target_comp)
        
        cr_d = self.__db.get_index()
        for id_comp in id_comps:
            outdf = self.__db.get_output(id_comp=id_comp)
            
            groupdf = outdf.groupby(outdf.index)
            median = groupdf['value'].median()
            median = self._moving_average(median, self.__mov_avg)
            cr_d = cr_d.join(median, how='left')
            
            cr_d = cr_d.rename(columns = {'value':'target'+str(id_comp)})
            
        return cr_d
    
    def load_data(self):
        data = self._load_inputs()
        return data
    
    def _load_PI(self, alpha):
        id_comps = self.__db.get_name_ids(self.__target_comp)

        pi_d = self.__db.get_index()
        for id_comp in id_comps:
            pi = self._prediction_intervals(id_comp, alpha)
            pi_d = pi_d.join(pi[0], how='left')
            pi_d = pi_d.rename(columns = {'value':'targetU'+str(id_comp)})
            pi_d = pi_d.join(pi[1], how='left')
            pi_d = pi_d.rename(columns = {'value':'targetL'+str(id_comp)})
            
        return pi_d
        
    def _load_cv(self):
        
        id_comps = self.__db.get_name_ids(self.__target_comp)
        
        cv_d = self.__db.get_index()
        for id_comp in id_comps:
            outdf = self.__db.get_output(id_comp=id_comp)
            
            groupdf = outdf.groupby(outdf.index)
            mn = groupdf['value'].mean()
            sd = groupdf['value'].std()
            mn = sd/mn
            mn = self._moving_average(mn, self.__mov_avg)
            cv_d = cv_d.join(mn, how='left')
            
            cv_d = cv_d.rename(columns = {'value':'target'+str(id_comp)})
            
        return cv_d
        
    def _load_range(self):
        
        id_comps = self.__db.get_name_ids(self.__target_comp)
        
        cr_d = self.__db.get_index()
        for id_comp in id_comps:
            outdf = self.__db.get_output(id_comp=id_comp)
            
            groupdf = outdf.groupby(outdf.index)
            maxval = groupdf['value'].max()
            minval = groupdf['value'].min()
            mn = (maxval-minval)#/(maxval+minval)
            mn = self._moving_average(mn, self.__mov_avg)
            cr_d = cr_d.join(mn, how='left')
            
            cr_d = cr_d.rename(columns = {'value':'target'+str(id_comp)})
            
        return cr_d
        
    def _load_std(self):
        
        id_comps = self.__db.get_name_ids(self.__target_comp)
        
        cv_d = self.__db.get_index()
        for id_comp in id_comps:
            outdf = self.__db.get_output(id_comp=id_comp)
            
            groupdf = outdf.groupby(outdf.index)
            std = groupdf['value'].std()

            std = self._moving_average(std, self.__mov_avg)
            cv_d = cv_d.join(std, how='left')
            cv_d = cv_d.rename(columns = {'value':'target'+str(id_comp)})
            
        return cv_d
        
    def _prediction_Q(self, id_comp, p):
        outdf = self.__db.get_output(id_comp=id_comp)
        return outdf.groupby([outdf.index]).quantile(p)
    
    def _transferred_prediction_Q(self, id_comp, p):
        #TODO: think how to do this without the filters
        #opt_out = self.__db.get_best_output(id_comp, self.__filter.objs, self.__filter.limits, self.__filter.operators)
        #q_out = self._prediction_Q(id_comp, p)
        #transferred = q_out['value'] - opt_out['value']
        #return transferred
        pass
    
    def _prediction_intervals(self, id_comp, alpha):
        PILower_out = self._transferred_prediction_Q(id_comp, (1-alpha))
        PIUpper_out = self._transferred_prediction_Q(id_comp, alpha)
        return [PILower_out,PIUpper_out]

    def _load_sd(self):
        id_comps = self.__db.get_name_ids(self.__target_comp)
        
        sd_d = self.__db.get_index()
        for id_comp in id_comps:
            outdf = self.__db.get_output(id_comp=id_comp)
            groupdf = outdf.groupby(outdf.index)
            sd = groupdf['value'].std()
            sd = self._moving_average(sd, self.__mov_avg)
            sd_d = sd_d.join(sd, how='left')
            
            sd_d = sd_d.rename(columns = {'value':'target'+str(id_comp)})
            
        return sd_d
    
    def _load_best(self, name, id_comp):
        best = self.__db.get_index()
        res = self.__db.get_best_output(id_comp=id_comp)
        res[['value']] = self._moving_average(res[['value']], self.__mov_avg)
        best = best.join(res[['value']], how='left')
        best = best.rename(columns = {'value':name})
        return best
        
    def _load_inputs(self):
        ids = self.__db.get_name_ids(self.__inputs)
        i = 0
        inputs = self.__db.get_index()
        
        for id_name in ids:
            name = self.__inputs[i]
            i = i + 1
            if self.__db._exists_meas(id_name):
                meas = self.__db.get_measurement(id_name)
            else:
                #If is not meas we want an optimized output
                meas = self._load_best(name, id_name)
            meas = meas.groupby(meas.index).sum() #Some events have same date
                             
            inputs = inputs.join(meas[['value']], how ='left')
            inputs = inputs.fillna(0)
           # inputs['ma'+name] = self._moving_average(inputs['value'], self.__mov_avg)
            inputs['ema'+name] = self.exp_average(inputs['value'], self.__mov_avg)
            inputs['value'] = self._moving_average(inputs['value'], self.__mov_avg)
            #inputs['value'] = inputs['value']
            
            inputs = inputs.rename(columns = {'value':name})
            
        return inputs
    
    def _moving_average(self,serie, n=7):
        result = serie.rolling(window=n,center=False,min_periods=1).mean()
        return result

    #TODO: explore more ways to get the previous data
    def exp_average(self, serie, n=7):
        result = serie.ewm(ignore_na=False,min_periods=1,adjust=True,com=n).mean()
        return result

        
    
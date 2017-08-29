"""
Created on June 2017

Responsability:
Execute Ldndc

@author: Ignacio Santabarbara
"""

import os
from datetime import datetime
import time
import sys
import logging
import numpy as np
import scipy as sci
import measurement as meas
import simulation as sim
import spotpy
from spotpyext import objectivefunctions as objfunext
import ldndc.config as config
from ldndc.persistence import Persistence
from inputs import ProjectFile


class Model(object):
    def __init__(self):
        # This function is optional for spotpy. One can load e.g. files here that have to be loaded only once
                
        self.owd = os.getcwd()
              
        logging.debug('Ini Model Setup')
        self.__rollwindow = 7
        self.__bin_path = config.parser.get('ldndc', 'binpath')
        self.__bin_exe = config.parser.get('ldndc', 'binexe')
        self.__meas_path = config.parser.get('ldndc', 'measurementspath')
        self.__model_inputs_path = config.parser.get('ldndc', 'modelinputspath')
        self.__site_path = config.parser.get('site', 'ldndcfile')
        
        self.__modelstart = datetime.strptime(config.parser.get('site', 'modelstart'), '%Y-%m-%d')  # Model starts
        self.__datastart = datetime.strptime(config.parser.get('site', 'datastart'), '%Y-%m-%d')  # Comparision with Meas data starts
        self.__dataend = datetime.strptime(config.parser.get('site', 'dataend'), '%Y-%m-%d')  # Model and comparision stops
        self.__rundays = (self.__dataend - self.__modelstart).days+1
        self.__year_timestamps = self._get_timestamp_ranges(self.__datastart,self.__dataend)
        
        #TODO: do we need params?
        param_file =  config.parser.get('prior', 'paramfile')
       
        self.__meas_loader = meas.Measurement(self.__meas_path, self.__modelstart, self.__datastart, self.__dataend)
        self.__measconf = config.parser.items("measurements")
        self.__meas_data = self._get_meas_data(rollwindow=self.__rollwindow)

        self.__obj_limits = dict(config.parser.items("filter"))
            
        self.__simconf = config.parser.items("simulations")
        
        #TODO: refactor the params
        dtype=[('module', 'S40'), ('name', 'S40'), ('distribution', 'S40'), ('init', '<f8'), ('min', '<f8'), ('max', '<f8')]
        self.__params = np.genfromtxt(param_file, names=True, dtype=dtype)
        self.__len_site_params = len(self.__params[np.where(self.__params['module']=='siteparams')])
        self.__len_soilinputs = len(self.__params[np.where(self.__params['module']=='soil')])
        self.__len_manainputs = len(self.__params[np.where(self.__params['module']=='mana')])
        
        #Functions to use in the objetivefunction
        self.__functions = [(lambda m,s,p: objfunext.nrmse(m.values,s.values),"nrmse"),
            # (lambda m,s,p: p * objfunext.nrmse(m.values,s.values),"pnrmse"),
             (lambda m,s,p: p,"p"), #Keep the penalization
             (lambda m,s,p: spotpy.objectivefunctions.rmse(m.values,s.values),"rmse"), 
             (lambda m,s,p: objfunext.mae(m.values,s.values),"mae"), 
             (lambda m,s,p: self._obj_yr_fun(m,s,np.sum,objfunext.eucl_dist,np.median),"yr_sum_eucl_median"), 
             (lambda m,s,p: self._obj_yr_fun(m,s,np.mean,objfunext.eucl_dist,np.median),"yr_mean_eucl_median"),
             (lambda m,s,p: 1/objfunext.r2_score(m.values,s.values),"r2_inv"), 
             (lambda m,s,p: objfunext.smape(m.values,s.values),"smape"),
             (lambda m,s,p: self._obj_yr_fun(m,s,None,objfunext.smape,np.mean),"yr_mean_smape"),
             (lambda m,s,p: self._obj_yr_fun(m,s,None,objfunext.smape,np.median),"yr_median_smape"),
             (lambda m,s,p: np.mean(s.values),"mean")]
             
        dbname =  config.parser.get('db', 'name')
        dbpath =  config.parser.get('db', 'path')
        
        self.__db = Persistence(dbpath, dbname, new=True)
        self._save_objetive_names()
        self._save_param_names()
        self._save_model_inputs()
        self._save_meas()
    
    def db(self):
        return self.__db
    def postprocess(self):
        ''' Creates the index in the database'''
        self.__db.create_indexes()
        self.__db.normalize_likelihood_table()
            
    def close(self):
        self.__db.close()
    
    def simulation(self,vector):
        ''' This function starts the model'''
        call = int(np.random.uniform(low=0,high=999999999))
        
        try:
            inputs = ProjectFile(self.__model_inputs_path, self.__site_path,
                                      call)
    
            sinkprefix = inputs.get_sinkprefix()
            inputs.set_call()
            inputs.set_schedule(self.__modelstart, self.__dataend, self.__rundays, 'dndc')
            paramvalues = vector[range(0,self.__len_site_params)]
            paramnames = self.__params[np.where(self.__params['module']=='siteparams')]['name']
            inputs.set_siteparams(paramvalues,paramnames)
    
            paramvalues = vector[range(self.__len_site_params, self.__len_site_params+self.__len_manainputs)]
            paramnames = self.__params[np.where(self.__params['module']=='mana')]['name']
            #inputs.set_manavalues(paramvalues,paramnames)
    
            paramvalues = vector[range(self.__len_site_params+self.__len_manainputs, self.__len_site_params+self.__len_manainputs+self.__len_soilinputs)]
            paramnames = self.__params[np.where(self.__params['module']=='soil')]['name']
            #inputs.set_soilvalues(paramvalues,paramnames)
            inputs.write()
    
            logging.debug('Start model...')
            starttime=time.time()
    
            os.chdir(self.__bin_path)
    
            ldndcfile = inputs._call_path()
            logging.debug(self.__bin_exe + " -c ldndc.conf " + ldndcfile)
            os.system(r"" + self.__bin_exe + " -c ldndc.conf " + ldndcfile)
            acttime=time.time()
            logging.debug('Duration:'+str(round((acttime-starttime),2))+' s')
    
            os.chdir(self.owd)
        except Exception, e:
            logging.error('Cannot execute simulation: ' + str(e))
        try:
            sim_data = self._read_simdata(sinkprefix, call=call)
            sim_data.append(vector)
            # Delete all model outputs, saves disk space
            try:
                inputs.remove_files()
            except:
                logging.error("Files could not be removed")
            return sim_data

        except:
            logging.error('Error: Loading of simulation data failed')
            print str(sys.exc_info()[1])
            logging.error(str(sys.exc_info()[1]))
            logging.warn('Returning nans...')
            os.chdir(self.owd)
            inputs.remove_files()
            evals = []
            for i in range(len(self.__meas_data)):
                evals.append(len(self.__meas_data[i])*[np.nan])
            
            return evals

    def evaluation(self):
        ''' This function loads the validation data'''
        eval_d = []
        for i in range(len(self.__meas_data)):
            eval_d.append(self.__meas_data[i][list(self.__meas_data[i])[0]])
    
        return eval_d

    def _get_timestamp_ranges(self,start,end):
        '''creates ranges for all the  evaluated years in timestamp type'''
        years = []
        for year in range(start.year,end.year+1):
            dt1 =  int(time.mktime(datetime(year=year, month=1, day=1).timetuple()))
            dt2 =  int(time.mktime(datetime(year=year, month=12, day=31).timetuple()))
            years.append((dt1,dt2))
        return years

    def _obj_fun_to_Q(self, eval_i, sim_i, objfun, Q=75):
        '''Apply the function fun to the data over the evaluation quartile Q'''
        Qval = np.percentile(eval_i.values, Q)
        eval_Q = eval_i[(eval_i.values >= Qval)]
        sim_Q = sim_i[(eval_i.values >= Qval)]
        q_nrmse = objfun(eval_Q.values,sim_Q.values)
        return q_nrmse
        
    def _obj_yr_fun(self, eval_i, sim_i, aggrfun, objfun, aggrresfun=np.mean):
        '''Apply an objective function to yearly data aggregated with datafun'''
        yrvals = []
        for years in self.__year_timestamps:
            eval_yr = eval_i.loc[(eval_i.index >= years[0]) & (eval_i.index <= years[1])]
            if len(eval_yr) > 0:
                sim_yr = sim_i.loc[(sim_i.index >= years[0]) & (sim_i.index <= years[1])]
                sim_yr = sim_yr.dropna()
                if aggrfun is None:
                    eval_yr = eval_yr.values
                    sim_yr = sim_yr.values
                else:
                    eval_yr = aggrfun(eval_yr.values)
                    sim_yr = aggrfun(sim_yr.values)
                yrvals.append(objfun(eval_yr,sim_yr))
        return aggrresfun(yrvals)

    def _obj_yr_fun_Q(self, eval_i, sim_i, objfun, Q):
        '''Apply an objective function to yearly data filtered by percetile Q'''
        yrvals = []
        for years in self.__year_timestamps:
            eval_yr = eval_i.loc[(eval_i.index >= years[0]) & (eval_i.index <= years[1])]
            if len(eval_yr) > 0:
                sim_yr = sim_i.loc[(sim_i.index >= years[0]) & (sim_i.index <= years[1])]
                val = self._obj_fun_to_Q(eval_yr,sim_yr, objfun, Q)
                yrvals.append(val)
        return np.mean(yrvals)
        
    def objectivefunction(self,simulation,evaluation):
        # This function is needed for spotpy to compare simulation and validation data
        # Keep in mind, that you reduce your simulation data to the values that can be compared with observation data
        # This can be done in the def simulation (than only those simulations are saved), or in the def objectivefunctions (than all simulations are saved)
        
        val_num = len(self.__functions)*len(evaluation)
        vals = []
        try:
            self.__db.incr_iteration()
            param_values = simulation.pop()
            self.__db.insert_param(self.__params['name'], param_values)
            
            validations = []
            for i in range(len(evaluation)):
                eval_i = evaluation[i]
                #The eval is already with a moving average
                sim_i = simulation[i].rolling(window=7, min_periods=1).mean()
                #Penalization including all simulation points, with or without meas
                #pena = self._penalization_yr_diff_ratio(eval_i,sim_i)
                pena = 1#self._penalization(eval_i,sim_i)
                #TODO: find a better solution when on copound is 0
                if np.isinf(pena):
                    pena = 10000
                
                #Remove simulation points without measurement points
                sim_i = sim_i.loc[sim_i.index.isin(eval_i.index)]      
                
                for idx, objfun in enumerate(self.__functions):
                    val = objfun[0](eval_i,sim_i,pena)
                    name = objfun[1]
                    if np.isinf(val) or np.isnan(val):
                        raise ValueError('Inf or Nan are not allow as objectivefunction value')
                    vals.append(val)
                    validations.append((name+str(i),val))
                        
                                
            self.__db.insert_obj(vals)
            self.__db.insert_sim(simulation)
                
        except Exception, e:
            vals = []
            for i in range(val_num):
                vals.append(float('inf'))
            logging.error("Objetive function fails: " + str(e))
        return vals
        
    def _save_model_inputs(self):
        '''Get the inputs of the model to save them in the database'''
        inputs = ProjectFile(self.__model_inputs_path, self.__site_path)
        inputs_d = inputs.load_data()
        self.__db.insert_meas(inputs_d)
    
    def _save_objetive_names(self):
        #TODO: make function names nicer
        funcnames = []
        for i in range(len(self.__meas_data)):
            for objfun in self.__functions:
                funcnames.append(objfun[1]+str(i))
        self.__db.set_objetive_names(funcnames)
        self.__db.insert_names(funcnames)
        
    def _save_param_names(self):
        names = self.__params['name']
        self.__db.insert_names(names)

                                  
    def _get_meas_data(self, rollwindow=7):
        ''' Read the measurement files '''
        meas_roll = []
        for name, meas_conf in self.__measconf:
            conf = str.split(meas_conf,"|")
            meas_roll.append(self.__meas_loader.read_and_rolling(measname=name, measfile=conf[0],
                                              multiplier=float(conf[1]), window=rollwindow))
                
        return meas_roll
        
    def _save_meas(self):
        ''' read the measurements without any modification and save them in the database'''
        meas = []
        for name, meas_conf in self.__measconf:
            conf = str.split(meas_conf,"|")
            meas.append(self.__meas_loader.read(measname=name, measfile=conf[0],
                                              multiplier=float(conf[1])))                      
        
        #Save measurements without rolling in db        
        eval_d = []                                  
        for i in range(len(meas)):
            eval_d.append(meas[i][list(meas[i])[0]])
        
        self.__db.insert_meas(eval_d)
            
    def _read_simdata(self,sinkprefix,call=None):
        sim_loader = sim.Simulation(self.__model_inputs_path)
        sim_loader.read(self.__simconf,sinkprefix, call=call)
        sim_data = []
        i = 0
        for colname, filename in self.__simconf:
            sim_data.append(sim_loader.getsim(colname=colname))
            i += 1

        return sim_data
        
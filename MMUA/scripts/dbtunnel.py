# -*- coding: utf-8 -*-
"""
Created on Sat Jun 10 14:19:51 2017

Script used to transform the R bayesian calibration results for the MMUA project
@author: ruiz-i
"""

import sys
import os
import sqlite3 as lite
import numpy as np
import pandas as pd
import logging
import time
from datetime import datetime

class Tunnel(object):
    def __init__(self, dbSrc, dbDest):
        self.__conSrc = lite.connect(dbSrc)
        self.__conDest = lite.connect(dbDest)
        
        self.__cache = {}
        self.__nonsimcols = ['chain','id','iteration','year','julianday','site', 'lik', 'likelihood']
    
   
    def close_src(self):
        self.__conSrc.close()
    def close_dest(self):
        self.__conDest.close()

    def process_names(self):
        names = self.get_src_param_names()
        self.insert_names(names)
        names = self.get_src_output_names()
        self.insert_names(names)
    
    def process_outputs(self):
        logging.debug("IN: process_outputs")
        names = self.get_src_output_names()
        for name in names['name']:
            if name not in self.__nonsimcols:
                id_val = self._get_or_insert_name(name)
                sim = self.get_src_output(name, id_val)
                sim = self.src_output_transform(sim)
                sim.to_sql(con=self.__conDest, name='Output', if_exists='append', index=True, index_label ="timestamp")
    
    def process_parameters(self):
        logging.debug("IN: process_parameters")
        names = self.get_src_param_names()
        for name in names['name']:
            if name not in self.__nonsimcols:
                id_val = self._get_or_insert_name(name)
                param = self.get_src_parameters(name, id_val)
                param.to_sql(con=self.__conDest, name='Parameter', if_exists='append', index=False)
    
    def process_likelihood(self):
        logging.debug("IN: process_likelihood")
        name = 'lik'
        id_val = self._get_or_insert_name(name)
        param = self.get_src_parameters(name, id_val)
        param.to_sql(con=self.__conDest, name='Likelihood', if_exists='append', index=False)
      
    def insert_names(self, names):
        for name in names['name']:
            self._get_or_insert_name(name)
    
    def _get_or_insert_name(self, name):
        '''Get the id for the cached name or insert it in the database if it doenst exist'''
        if name in self.__cache:
            id_val = self.__cache[name]
        else:
            cursor = self.__conDest.cursor()
            insertcmd = "INSERT INTO Name (name) VALUES (?)" 
            cursor.execute(insertcmd, (name,))
            id_val = int(cursor.lastrowid)
            self.__cache[name] = id_val
        return id_val
        
    def get_src_param_names(self):
        with self.__conSrc:
            sqlcmd = "PRAGMA table_info(Parameters)"
            logging.info(sqlcmd)
            return pd.read_sql(sqlcmd, con=self.__conSrc, columns =['name'])
        
    def get_src_output_names(self):
        with self.__conSrc:
            sqlcmd = "PRAGMA table_info(Outputs)"
            logging.info(sqlcmd)
            return pd.read_sql(sqlcmd, con=self.__conSrc, columns =['name'])
            
    def get_src_output(self, col, id_val):
        with self.__conSrc:
            sqlcmd = "SELECT " +  str(id_val) + " as id_name, iteration, (cast(year as integer) || ' ' || cast(julianday as integer)) AS timestamp, " + col + " as value FROM Outputs"
            logging.info(sqlcmd)
            return pd.read_sql(sqlcmd, con=self.__conSrc)
    
    def src_output_transform(self, out):
        logging.debug("IN: src_output_transform")
        date2ts = lambda x: int(time.mktime(datetime.strptime(x, '%Y %j').timetuple()))
        out['timestamp'] = list(map(date2ts, out['timestamp']))
        out = out.set_index('timestamp')
        return out
       # out['year'].delete()
       # out['julianday'].delete()
        
    def get_src_parameters(self, col, id_val):
        with self.__conSrc:
            sqlcmd = "SELECT " +  str(id_val) + " AS id_name, iteration, " + col + " AS value FROM Parameters"
            logging.info(sqlcmd)
            return pd.read_sql(sqlcmd, con=self.__conSrc)
            
   

if __name__ == "__main__":

    logging.basicConfig(format='%(asctime)s %(message)s')
    logging.basicConfig(filename='c:\projects\ML\MMUA\dbtunnel.log',level=logging.DEBUG)
    
    logging.info('Starting the database conversion')
    
    try:
        dbsrc =  'C:\projects\ML\databases\UGX2525AVG.db'
        dbdest = 'C:\projects\ML\databases\UGXML.db'

        tunnel = Tunnel(dbsrc, dbdest)
        
        tunnel.process_names()
        tunnel.process_outputs()
        tunnel.process_parameters()
        tunnel.process_likelihood()
        logging.info('Finish the database conversion')
        #TODO: process likelihoods
    except Exception:
        logging.error("General error" + str(sys.exc_info()[1]))


    finally:
        logging.info('Closing databases')
        tunnel.close_src()
        tunnel.close_dest()
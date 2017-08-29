# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 14:40:37 2017

Responsability: execute the plots
@author: ruiz-i
"""

import logging
import os
import spotpy
import ldndc.spot
import ldndc.config as config
from mpi4py import MPI
import sys
from ldndc.objfilter import Filter
from ldndc.persistence import Persistence
from ldndc.plot import Plot

if __name__ == "__main__":

    
    
    config_file = str(sys.argv[1])
    
    
    config.parser = config.set(config_file)
    logfile = config.parser.get('global', 'logfile')
    loglevel = config.parser.get('global', 'loglevel')
    logging.basicConfig(filename=logfile,level=loglevel)
    logging.debug("config file:"+ config_file)
    logging.info("Start!")
    dbname = config.parser.get('db', 'name')
    
    try:
        dbname =  config.parser.get('db', 'name')
        dbpath =  config.parser.get('db', 'path')
        objFilter = Filter(config.parser.items('filter'))
        db = Persistence(dbpath, dbname, new=False)
        #db.create_indexes()
        plotter = Plot(db, objFilter)
        #plotter.normtest()
        #plotter.plotOutput('dNn2oemiskgNha1', 'n_n2o')
        plotter.plotBestOutput('dN_n2o_emis[kgNha-1]', 'n_n2o')
        #plotter.plotBestOutput('dNn2oemiskgNha1', 'n_n2o')
        #plotter.plotOutput('dNnoemiskgNha1', 'n_no')
        plotter.plotBestOutput('dNnoemiskgNha1', 'n_no')
                #plotter.plotOutput('N_nh4[kgNha-1]', 'n_nh4')  
       # plotter.plotBestOutput('N_nh4[kgNha-1]', 'n_nh4')
        
        #plotter.plotOutput('dC_co2_emis_auto[kgCha-1]+dC_co2_emis_hetero[kgCha-1]', 'c_co2')
     #   plotter.plotBestOutput('dC_co2_emis_auto[kgCha-1]+dC_co2_emis_hetero[kgCha-1]', 'c_co2')
        #plotter.plotOutput('N_no3[kgNha-1]', 'n_no3')  
      #  plotter.plotBestOutput('N_no3[kgNha-1]', 'n_no3')

       # plotter.plotFilteredGrid('dN_n2o_emis[kgNha-1]', 'n_n2o')
       # plotter.plot1rtFilterOutput('N_no3[kgNha-1]', 'n_no3')
    except Exception, e:
        logging.error(str(e))
    finally:
        db.close()

# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 14:40:37 2017

@author: ruiz-i
"""

import logging
import os
import spotpy
import ldndc.spot
import ldndc.config as config
from mpi4py import MPI
import sys


if __name__ == "__main__":

    
    
    config_file = str(sys.argv[1])
    
    
    config.parser = config.set(config_file)
    logfile = config.parser.get('global', 'logfile')
    loglevel = config.parser.get('global', 'loglevel')
    logging.basicConfig(filename=logfile,level=loglevel)
    logging.debug("config file:"+ config_file)
    logging.info("Start!")
    dbname = config.parser.get('db', 'name')
    
    rep=int(config.parser.get('global', 'rep'))
    
    try:
        comm = MPI.COMM_WORLD
        size = comm.Get_size()
        if size > 1:
            parallel = "mpi"
        else:
            parallel = "seq"
    except KeyError:
        parallel = "seq"
    logging.info("MPI? Size:" + str(size) + " " + parallel)
   
    setup = None
    try:
        
        setup = ldndc.spot.Setup()
        sampler = spotpy.algorithms.lhs(setup, dbname=dbname, dbformat='csv', parallel= parallel, save_sim=False)
        # Run the analysis and save results
        sampler.sample(rep)
        #Actions after the running
        setup.postprocess()
    except Exception, e:
        logging.error(str(e))
    finally:
        setup.close()

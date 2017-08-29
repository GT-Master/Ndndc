# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 14:40:37 2017

Take the parameters from a calibrated database and use then to create a new
database with the executions of other site.
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
        

        # Run the analysis and save results
        sampler.sample(rep)
    except Exception, e:
        logging.error(str(e))
    finally:
        setup.close()

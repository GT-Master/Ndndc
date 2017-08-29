# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 16:31:10 2017

@author: ruiz-i
"""

import ConfigParser

def set(config_file):
    parser = ConfigParser.RawConfigParser()
    parser.optionxform = str 
    parser.read(config_file)
    return parser



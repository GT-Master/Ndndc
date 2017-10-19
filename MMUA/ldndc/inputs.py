# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 16:47:31 2017
Responsability: 
Load and read the ldndc input files
@author: ruiz-i
"""
import os 
import sys
import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
import logging
import re
import time
from datetime import datetime

class ProjectFile(object):
    ''' Load and read the ldndc project file '''
    def __init__(self,modelinputspath,projectpath, call=None):
        self.__modelinputs_path = modelinputspath
        self.__project_path = projectpath
        self.__sinkprefix = None
        self.owd = os.getcwd()
        self.__call = call
        self.__tree = None
        self.__root = None
        self.open()
        self.__mana = ManaFile(modelinputspath, self._get_mana(), call)
        site = self._get_site()
        
        self.__site = SiteFile(modelinputspath, site, call)
        siteparameters = site.replace(".xml", "params.xml")
        self.__site_parameters = SiteParametersFile(modelinputspath, siteparameters, call)
        
        self.__climate = ClimateFile(modelinputspath, self._get_climate(), call)
        self.__airchem = AirchemFile(modelinputspath, self._get_airchem(), call)
    
    def open(self):
        self.__tree = ET.parse(self._original_path())
        self.__root = self.__tree.getroot()
        
    def load_data(self, aslist=True):
        '''get the data from input files'''
        data = self.__mana.load_data() + self.__climate.load_data()
        return data
        
    def load_dataframe(self):
        ''' get the data in a dataframe structure'''
        data = self.__climate.load_dataframe()
        data = data.mask(data < -90)
        events = self.__mana.load_data()
        for event in events:
            event = event.groupby(event.index).sum() #Some events have same
            data = data.join(event.ix[:,0], how ='left')
            data = data.fillna(0)
        props = self.load_properties()
        for key in props:
            data[key] = props[key]
        
        return data
        
        
    def load_properties(self):
        ''' get not dated data '''
        props = {}
        #props['longitude'] = self.__climate.get_longitude()
        #props['latitude'] = self.__climate.get_latitude()
        props['elevation'] = self.__climate.get_elevation()
        props['ph'] = self.__site.get_ph_avg()
        props['clay'] = self.__site.get_clay_avg()
        props['corg'] = self.__site.get_corg_avg()
        props['norg'] = self.__site.get_norg_avg()
        props['bd'] = self.__site.get_bd_avg()
        #props['scel'] = self.__site.get_scel_avg()
        props['soil'] = self.__site.get_soil()
        props['usehistory'] = self.__site.get_usehistory()
        props['wcavg'] = (self.__site.get_wcmax_avg() + self.__site.get_wcmin_avg())/2
        #props['wcmax'] = self.__site.get_wcmax_avg()
        #props['wcmin'] = self.__site.get_wcmin_avg()
        props.update(self.__airchem.load_data())
        return props
        
    def set_call(self):
        self._set_call_outputsinks()
        self._set_call_site()
        self._set_call_mana()
        #self._set_call_source()
        self._set_call_siteparameters()
    
    def write(self):
        ''' Write all modifications in the new 'call' project file '''
        file_path = self._call_path()
        self.__tree.write(file_path)
        
        #Workaound to fix the > symbol inside the xml
        filedata = None
        with open(file_path, 'r') as file:
            filedata = file.read()
        filedata = filedata.replace('&gt;', '>')
        with open(file_path, 'w') as file:
            file.write(filedata)
        
        self.__mana.write()
        self.__site.write()
    
    def _original_path(self):
        ''' Get original project file path'''
        return self.__modelinputs_path + os.sep + self.__project_path
    
    def _call_path(self):
        ''' Get the new project file path that has the needed modifiations'''
        return self.__modelinputs_path + os.sep + self.__project_path.replace(".ldndc", str(self.__call) +".ldndc")
    
    def set_siteparams(self,values, params):
        self.__site_parameters.create(values, params)
        
    def set_manavalues(self,values, params):
        self.__mana.set_params(values, params)
        self.__mana.write()
        
    def set_soilvalues(self,values, params):
        self.__site.set_params(values, params)
        self.__site.write()
        
    def remove_files(self):
        try:
            os.remove(self.__site._call_path())
            os.remove(self.__mana._call_path())
            os.remove(self.__site_parameters._call_path())
            os.remove(self._call_path())
        except OSError:
            print str(sys.exc_info()[1])
            logging.error('Remving files:' + str(sys.exc_info()[1]))

    def set_schedule(self, start, end, rundays, module='dndc'):
        xpath = "./schedule"
        for e in self.__root.findall(xpath):
            if module == 'dndc':
                e.set('time', (str(start.year) + '-' + str(start.month) + '-' + str(start.day) + '/1 -> +' + str(
                        rundays)))
            else:
                e.set('time', (str(start.year) + '-' + str(
                        start.month) + '-' + str(start.day) + '/24 -> +' + str(
                        rundays)))

    def get_sinkprefix(self):
        xpath = "./output/sinks"
        sinkprefix = self.__root.find(xpath).attrib['sinkprefix']
        return sinkprefix
        
    def _set_call_outputsinks(self):
        xpath = "./output/sinks"
        self.__sinkprefix = self.__root.find(xpath).attrib['sinkprefix']
        for e in self.__root.findall(xpath):
            e.set('sinkprefix', (self.__sinkprefix + str(self.__call) + "_"))
            self.__sinkprefix = (self.__sinkprefix + str(self.__call) + "_")
    
    def _set_call_site(self):
        xpath = "./input/sources/site"
        self._set_call_source(xpath, 'source')
    
    def _set_call_mana(self):
        xpath = "./input/sources/event"
        self._set_call_source(xpath, 'source')

    def _set_call_speciesparameters(self):
        xpath = "./input/sources/speciesparameters"
        if not self._set_call_source(xpath, 'source'):
            source = self._get("./input/sources/site", "source").replace("site.xml", "speciesparams" + str(self.__call) + ".xml")
            for e in self.__root.findall("./input/sources"):
                ET.SubElement(e,'speciesparameters').set('source',source)
    
    def _set_call_siteparameters(self):
        xpath = "./input/sources/siteparameters"
        if not self._set_call_source(xpath, 'source'):
            source = self._get("./input/sources/site", "source").replace(str(self.__call) + ".xml", "params" + str(self.__call) + ".xml")
            for e in self.__root.findall("./input/sources"):
                ET.SubElement(e,'siteparameters').set('source',source)

    def _set_call_source(self, xpath, att='source'):
        source = self._get(xpath, att)
        mod = False
        for e in self.__root.findall(xpath):
            mod = True
            e.set(att, source.replace(".xml", str(self.__call) + ".xml"))
        return mod

    def _get_site(self):
        xpath = "./input/sources/site"
        return self._get_inputs_path(xpath)
        
    def _get_mana(self):
        xpath = "./input/sources/event"
        return self._get_inputs_path(xpath)
    
    def _get_climate(self):
        xpath = "./input/sources/climate"
        return self._get_inputs_path(xpath)
    
    def _get_airchem(self):
        xpath = "./input/sources/airchemistry"
        return self._get_inputs_path(xpath)
        
    def _get_inputs_path(self, xpath):
        site_source = self._get(xpath, "source")
        site_sourceprefix = self._get_sourceprefix(xpath)
        site_path = site_sourceprefix + site_source
        return site_path#.replace(".xml", str(self.__call) + ".xml")
    
    def _get_sourceprefix(self,xpath):
        srcprefix = self._get(xpath, "sourceprefix")
        if srcprefix is None:
            srcprefix = self._get(".input/sources", "sourceprefix")
        return srcprefix
                
    def _get(self, xpath, att):
        ele = self.__root.find(xpath)
        if ele is not None:
            return ele.get(att)
        return None
    
class SiteParametersFile(object):
    ''' Creates a new siteparameter file'''
    def __init__(self, modelinputspath, path, call):
        self.modelinputs_path = modelinputspath
        self.path = path
        self.__call = call
    
    def _call_path(self):
        ''' Get the new siteparams file path that has the needed modifiations'''
        return self.modelinputs_path + os.sep + self.path.replace(".xml", str(self.__call)+".xml")
        
    def create(self,values, params):
        '''Creates a new xml file for the new site parameter values'''
        filepath = self._call_path()
        out = open(filepath, 'w')    
        out.write('<?xml version="1.0" ?>\n')
        out.write('<ldndcsiteparameters>\n')
        out.write('\n')
        out.write('    <siteparameters id="0" >\n')
        out.write('\n')
        for i in range(len(values)):
            out.write('    <par name="'+str(params[i])+'" value="'+str(values[i])+'" source="orig." />\n')
        out.write('\n') 
        out.write('    </siteparameters> \n')
        out.write('</ldndcsiteparameters>\n')
        out.close()   
        
class ManaFile(object):
    def __init__(self, modelinputspath, path, call):
        self.modelinputs_path = modelinputspath
        self.path= path
        self.__call = call
        self.__tree = None
        self.__root = None
        self.open()
                
    def open(self):
        self.__tree = ET.parse(self._original_path())
        self.__root = self.__tree.getroot()
    
    def write(self):
        ''' Write all modifications in the new 'call' mana file '''
        file_path = self._call_path()
        self.__tree.write(file_path)
        
        #Workaound to fix the > symbol inside the xml
        filedata = None
        with open(file_path, 'r') as file:
            filedata = file.read()
        filedata = filedata.replace('&gt;', '>')
        with open(file_path, 'w') as file:
            file.write(filedata)

    def load_data(self):
        #TODO: normalize events
        events_att = {'fertilize': lambda node: float(node.attrib['amount']),
                      'till': lambda node: float(node.attrib['depth']),
                      'manure':lambda node: float(node.attrib['availn'])*float(node.attrib['c'])/float(node.attrib['cn']), #TODO: decide how to get comparable amount as fertilize
                      'harvest':lambda node: float(node.attrib['remains']),
#TODO: hope <crop is always the first node
                      'plant': lambda node: float(node[0].attrib['initialbiomass'])}
        data = {'fertilize': [],
                'till': [],
                'harvest':[],
                'plant':[]}
                
        xpath = ".//event"
        for e in self.__root.findall(xpath):
            #TODO: fertilize dates are repeated, several events per date
        #TODO: ask edwin for meaning of fertilize types: amount is the N amount, the problem is that each fertilizer has different times to be active.
        #Should I use type instead of general fertilize
            typ = e.get("type")
            
            datets =  int(time.mktime(datetime.strptime(e.get("time"), '%Y-%m-%d').timetuple()))    
            if typ in events_att:
                
                node = list(e)[0]
                if node.tag==typ:
                    value =  events_att[typ](node)

                    #value = float(node.attrib[att])
                    if typ == 'manure':
                        typ = 'fertilize'
                    data[typ].append((value,datets))
        
        mana_d = []
        for col in data.keys():
            df = pd.DataFrame(data[col], columns=[col,'date'])
            df = df.set_index('date')
            mana_d.append(df)
        return mana_d
    
    def _original_path(self):
        ''' Get original mana file path'''
        return self.modelinputs_path + os.sep + self.path
    
    def _call_path(self):
        ''' Get the new mana file path that has the needed modifiations'''
        return self.modelinputs_path + os.sep + self.path.replace(".xml", str(self.__call)+".xml")
    
    def set_params(self, values, params):
        for i in range(len(values)):
            permod = values[i]
            eventname = params[i]
            xpath = ".//" + eventname
            for e in self.__root.findall(xpath):
                value = float(e.get("amount")) 
                newvalue = round(value + value * permod, 5)
                e.set('amount', str(newvalue))
        
class SiteFile(object):
    
    ''' Load and read the ldndc site file '''
    def __init__(self, modelinputspath, path, call):
        self.modelinputs_path = modelinputspath
        self.path= path
        self.__call = call
        self.__tree = None
        self.__root = None
        self.open()
    
    def open(self):
        self.__tree = ET.parse(self._original_path())
        self.__root = self.__tree.getroot()
    
    def write(self):
        ''' Write all modifications in the new 'call' mana file '''
        file_path = self._call_path()
        self.__tree.write(file_path)
        
        #Workaound to fix the > symbol inside the xml
        filedata = None
        with open(file_path, 'r') as file:
            filedata = file.read()
        filedata = filedata.replace('&gt;', '>')
        with open(file_path, 'w') as file:
            file.write(filedata)
    
    def _original_path(self):
        ''' Get original mana file path'''
        return self.modelinputs_path + os.sep + self.path
    
    def _call_path(self):
        ''' Get the new mana file path that has the needed modifiations'''
        return self.modelinputs_path + os.sep + self.path.replace(".xml", str(self.__call)+".xml")
     
    def set_params(self, values, params):
        for i in range(len(values)):
            permod = values[i]
            atts = params[i]
            
            if atts == 'corg':
                atts = ['corg','norg']
            elif atts == 'wcmax':
                atts = ['wcmax','wcmin']
            else:
                atts = [atts]
                
            for att in atts:
                xpath = ".//*[@"+att+"]"
                for e in self.__root.findall(xpath):
                    value = e.get(att) 
                    if value is not None:
                        newvalue = round(float(value) + float(value) * permod,5)
                        e.set(att, str(newvalue))
    
    def att_avg(self, att):
        ''' Read all the att values and average them '''
        values = []
        xpath = ".//*[@"+att+"]"
        for e in self.__root.findall(xpath):
            value = e.get(att)
            if value is not None:
                values.append(float(value))
        return np.mean(values)
    
    def att_str(self, att):
        ''' Read the att value and return '''
        xpath = ".//*[@"+att+"]"
        for e in self.__root.findall(xpath):
            value = e.get(att)
            return value
        return None
    
    def get_usehistory(self):
        return self.att_str("usehistory")

    def get_soil(self):
        return self.att_str("soil")
        
    def get_ph_avg(self):
        return self.att_avg("ph")
    
    def get_corg_avg(self):
        return self.att_avg("corg")
    
    def get_norg_avg(self):
        return self.att_avg("norg")

    def get_bd_avg(self):
        return self.att_avg("bd")

    def get_clay_avg(self):
        return self.att_avg("clay")   

    def get_scel_avg(self):
        return self.att_avg("scel")
    
    def get_wcmax_avg(self):
        return self.att_avg("wcmax")   

    def get_wcmin_avg(self):
        return self.att_avg("wcmin")           
        
                        
class ClimateFile(object):
    '''Prepare the climate data in a dataframe format'''
    def __init__(self,modelinputspath,path, call=None):
        self.__modelinputs_path = modelinputspath
        self.__path   = path
    
    def _original_path(self):
        ''' Get original mana file path'''
        return self.__modelinputs_path + os.sep + self.__path

    def load_data(self):
        '''loads climate from file in a list of columns'''
        
        dataline = self._get_data_line()
        colnames = self._get_cols_line(dataline)
        data = self.load_dataframe()
        
        clima_d = []
        for col in colnames:
            if col != 'date':
                clima_d.append(data[col])
        return clima_d
    
    def load_dataframe(self):
        ''' Load a dataframe with the climate'''
        dataline = self._get_data_line()
        colnames = self._get_cols_line(dataline)
        if colnames[0] == 'date':
            date2ts = lambda x: int(time.mktime(datetime.strptime(x, '%Y-%m-%d').timetuple()))
            data = pd.read_csv(self._original_path(), sep="\t", skiprows=dataline, usecols=colnames,
                               converters={'date':date2ts}, index_col=['date'])
        else:
            date2ts = lambda x: int(time.mktime(x.timetuple()))
            start_date = self._get_first_date()
            data = pd.read_csv(self._original_path(), sep="\t", skiprows=dataline, usecols=colnames)
            data['date'] = map(date2ts, pd.date_range(start_date, periods=len(data)))
            data = data.set_index('date')
        
        return data
        
    def _get_first_date(self):
        '''Get the first date'''
        with open(self._original_path()) as f:
            match=re.search(r'(\d+-\d+-\d+)',f.readlines()[1])
            date = match.group(1)
            return datetime.strptime(date, '%Y-%m-%d')
            
    def _get_cols_line(self, colslinenum):
        '''Get the column names in a list'''
        with open(self._original_path()) as f:
            line = f.readlines()[colslinenum]
            return map(str.strip,re.split(r'\t',line))
            
    def _get_data_line(self):
        '''Find where the data starts'''
        with open(self._original_path()) as f:
            for num, line in enumerate(f, 1):
                if '%data' in line:
                    return num
        return -1

    def _find_value(self, att):
        ''' Get the longitude value '''
        datanum = self._get_data_line()
        
        with open(self._original_path()) as f:
            for num, line in enumerate(f, 1):
                print line
                if att in line:
                    m = re.search("\d+.\d+",line)
                    #TODO: exceptions?
                    num = float(m.group(0))
                    return num
                if num == datanum-1:
                    return None
        
    def get_longitude(self):
        ''' Get the longitude value '''
        return self._find_value("longitude")
    
    def get_latitude(self):
        return self._find_value("latitude")
    
    def get_elevation(self):
        return self._find_value("elevation")
    
class AirchemFile(object):
    '''Prepare the airchem data in a dataframe format'''
    def __init__(self,modelinputspath,path, call=None):
        self.__modelinputs_path = modelinputspath
        self.__path   = path
        self.__call = call
    
    def _original_path(self):
        ''' Get original mana file path'''
        return self.__modelinputs_path + os.sep + self.__path

    def _call_path(self):
        ''' Get the new airchem file path that has the needed modifiations'''
        return self.__modelinputs_path + os.sep + self.__path.replace(".txt", str(self.__call)+".txt")
     
    def load_data(self):
        '''loads airchem from file to a dict'''
        self._format()
        colnames = self._get_cols_line()
        data = pd.read_csv(self._call_path(), sep="\t", usecols=colnames)
        
        airchem_d = {}
        for col in colnames:
            if col != 'date':
                airchem_d[col] = data[col].mean()
                
        return airchem_d
            
    def _get_cols_line(self):
        '''Get the column names in a list'''
        with open(self._call_path()) as f:
            line = f.readlines()[0]
            return map(str.strip,re.split(r'\t',line))
            
    def _format(self, sep="\t"):
        with open(self._call_path(), 'w') as outfile:
          with open(self._original_path()) as infile:
            for line in infile:
               fields = line.strip().split()
               print >>outfile, '\t'.join(fields)


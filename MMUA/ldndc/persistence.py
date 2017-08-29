# -*- coding: utf-8 -*-
"""
Created on Tue Mar 21 15:22:33 2017
Responsability: handle SQL communication
@author: ruiz-i
"""
import os
import sqlite3 as lite
import numpy as np
import pandas as pd
import logging

#TODO: Should select with likelihood being deprecated? And only use the normlikelihood for selects

class Persistence(object):
    def __init__(self, path, dbfile, new=True):
        """Creates the connection with the sqlite database and create the table structure 
        if it doesnt exists"""
        logging.info('Creating connection to db')
        if new:
            self._backup_old(path, dbfile)
        print path + os.sep + dbfile + ".db"
        self.__con = lite.connect(path + os.sep + dbfile + ".db")
        self._create_structure()
        
        self.__cache = {}
        self.__objetive_names = None
        self.__iteration = 0
        logging.info("Init persistence with start iteration: " + str(self.__iteration))

    def close(self):
        self.__con.close()
    
    def _backup_old(self, path, dbfile):
        '''If the db file exists we rename it to start a new one'''
        try:
            old = path + os.sep + dbfile + ".db"
            rnd = int(np.random.uniform(low=0,high=999999999))
            new = path + os.sep + dbfile + str(rnd) + ".db"
            os.rename(old, new)
        except Exception, e:
            logging.warn('Cannot backup the db file.' + str(e))
        
    def _create_structure(self):
        """Create the table of the output database"""
        self._create("CREATE TABLE IF NOT EXISTS Output (timestamp INTEGER, value REAL, id_name INTEGER, iteration INTEGER)")
        
        self._create("CREATE TABLE IF NOT EXISTS Measurement (timestamp INTEGER, id_name INTEGER, value REAL)")
        self._create("CREATE TABLE IF NOT EXISTS Likelihood (iteration INTEGER, id_name INTEGER, value REAL)")
        self._create("CREATE TABLE IF NOT EXISTS Parameter (iteration INTEGER, id_name INTEGER, value REAL)")
        self._create("CREATE TABLE IF NOT EXISTS Name (id_name INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
    
    def _get_or_insert_name(self, name):
        '''Get the id for the cached name or insert it in the database if it doenst exist'''
        if name in self.__cache:
            id_val = self.__cache[name]
        else:
            cursor = self.__con.cursor()
            insertcmd = "INSERT INTO Name (name) VALUES (?)" 
            cursor.execute(insertcmd, (name,))
            id_val = int(cursor.lastrowid)
            self.__cache[name] = id_val
        return id_val
        
    def _get_name(self, name):
        sqlcmd = "SELECT id_name FROM Name WHERE name = '" + name + "'"
        cursor = self.__con.cursor()
        cursor.execute(sqlcmd)
        res = pd.DataFrame(cursor.fetchall(), columns=['id_name'])
        if len(res) == 0:
            return None
        else:
            return int(res['id_name'].values[0])
        
    def _exists_meas(self, id_name):
        sqlcmd = "SELECT id_name FROM Measurement WHERE id_name = " + str(id_name)
        cursor = self.__con.cursor()
        cursor.execute(sqlcmd)
        res = pd.DataFrame(cursor.fetchall(), columns=['id_name'])
        return len(res['id_name']) != 0
        
    def _create(self, sqlcmd):
        '''Execute the command with a sqlite cursor object'''
        with self.__con:
            cursor = self.__con.cursor()
            cursor.execute(sqlcmd)

    def _table_exists(self, table):
        sql = "SELECT name FROM sqlite_master WHERE type='table' AND name='" + table+"'"
        exists = False
        with self.__con:
            cursor = self.__con.cursor()
            
            cursor.execute(sql)
            rows = cursor.fetchall()
            exists = len(rows) > 0
        return exists
            
    def create_indexes(self):
        
        sqlindexes = ["CREATE INDEX IF NOT EXISTS output_name ON Output (id_name)",
             
                      "CREATE INDEX IF NOT EXISTS likelihood_name ON Likelihood (id_name)",
                      "CREATE INDEX IF NOT EXISTS likelihood_value ON Likelihood (id_name, value)",
                      "CREATE INDEX IF NOT EXISTS output_iteration ON Output (iteration)",
                      
                      "CREATE INDEX IF NOT EXISTS likelihood_iteration ON Likelihood (iteration)",
                      "CREATE INDEX IF NOT EXISTS parameter_iteration ON Parameter (iteration)"]

        for sqlindex in sqlindexes:
            self._create(sqlindex)
        
    def _insert_sim(self, simulation, table="Output"):
        """Insert the simulation results"""
        with self.__con:
            logging.info("Insert simulation for iteration: " + str(self.__iteration))
            for i in range(len(simulation)):
                try:
                    sim = simulation[i].to_frame()
                    id_name = self._get_or_insert_name(sim.columns[0])
                    sim['id_name'] = np.repeat(id_name,len(sim))
                    sim['iteration'] = np.repeat(self.__iteration,len(sim))
                    sim.columns = ['value','id_name','iteration']
                    sim.to_sql(con=self.__con, name=table, if_exists='append', index=True, index_label ="timestamp")
                except Exception, e:
                    logging.error('Failed to insert simulation ' + str(self.__iteration) + str(e))
    
    def insert_sim(self, simulation):
        self._insert_sim(simulation)
        
    

    def insert_meas(self, measurement):
        """Insert the measurements"""
        with self.__con:
            for i in range(len(measurement)):
                meas = measurement[i]
                if not type(meas) == pd.DataFrame:
                    meas = meas.to_frame()
                id_name = self._get_or_insert_name(meas.columns[0])
                if not self._exists_meas(id_name):
                    meas['id_name'] = np.repeat(id_name,len(meas))
                    meas.columns = ['value','id_name']
                    meas.to_sql(con=self.__con, name='Measurement', if_exists='append', index=True, index_label ="timestamp")
    
    def set_objetive_names(self,names):
        self.__objetive_names = names
        
    def _name_obj_index(self, i):
        '''Get a name for the obj from an index'''
        return self.__objetive_names[i]#"like"+str(i)
    
    def insert_obj(self, vals):
        """Insert the objective function results"""
        with self.__con:
            for i in range(len(vals)):
                obj_name = self._name_obj_index(i)
                id_name = self._get_or_insert_name(obj_name)
                sqlinsert = "INSERT INTO Likelihood VALUES (?,?,?)" #iteration, self.stats[obj_name], self.compounds[comp_name], val)"
                cursor = self.__con.cursor()
                cursor.execute(sqlinsert, (self.__iteration, id_name, vals[i]))

    def incr_iteration(self):
        self.__iteration += 1
        logging.info("Increased iteration, new value:" + str(self.__iteration))
        
    def get_iteration(self):
        logging.info("Get iteration, value:" + str(self.__iteration))
        return self.__iteration
        
    def insert_param(self, names, values):
        """Insert the used parameters"""
        with self.__con:
            for i in range(len(values)):
                name = names[i]
                value = values[i]
                id_name = self._get_or_insert_name(name)
                sqlinsert = "INSERT INTO Parameter VALUES (?,?,?)"
                cursor = self.__con.cursor()
                cursor.execute(sqlinsert, (self.__iteration, id_name, value))
            
    def insert_names(self, names):
        with self.__con:
            for i in range(len(names)):
                name = names[i]
                self._get_or_insert_name(name)

    def get_likelihood(self, id_obj = None):
        with self.__con:
            if id_obj is None:
                sqlcmd = "SELECT * FROM Likelihood"
            else:
                sqlcmd = "SELECT * FROM Likelihood WHERE id_name =" + str(id_obj)
            logging.info(sqlcmd)
            return pd.read_sql(sqlcmd, con=self.__con)
    
    def get_likelihood_from_outputs(self, id_obj=None):
        '''Get the likelihoods of iterations where some outputs were saved '''
        with self.__con:
            if id_obj is None:
                sqlcmd = "SELECT * FROM Likelihood WHERE iteration IN (SELECT DISTINCT iteration FROM Output)"
            else:
                sqlcmd = "SELECT * FROM Likelihood WHERE id_name = " + str(id_obj) + " AND iteration IN (SELECT DISTINCT iteration FROM Output)"
            logging.info(sqlcmd)
            return pd.read_sql(sqlcmd, con=self.__con)
        
    #TODO: Deprecated?
    def get_likelihood_by_objindex(self, obj_index=None):
        obj_name = self._name_obj_index(obj_index)
        id_o = self._get_or_insert_name(obj_name)
        return self.get_likelihood(id_o)
        
    def _get_likelihood_names(self):
        ''' Get the names of the likelihoods'''
        with self.__con:
            #TODO: I assume we use METRX parameters
            sqlcmd = "SELECT id_name, name FROM name WHERE id_name = 1"#< (SELECT min(id_name) FROM name WHERE name like 'METRX%')"
            res = pd.read_sql(sqlcmd, con=self.__con)
            return res
    
    def normalize_likelihood_table(self):
        ''' To avoid many joins we create new table for normalized values'''
        #self._create('DROP TABLE NormLikelihood')
        colnames = self._get_likelihood_names()
        sqlnew = "CREATE TABLE IF NOT EXISTS NormLikelihood (iteration INTEGER"
        for index, row in colnames.iterrows():
            sqlnew = sqlnew + ", " + row['name'] + " REAL"
        sqlnew = sqlnew + ")"
        self._create(sqlnew)
        
        for index, row in colnames.iterrows():
            id_obj = int(row['id_name'])
            name = row['name']
            likelihoods = self.get_likelihood_from_outputs(id_obj)
            try:
                #TODO: likelihoods['value'] = 1/likelihoods['value'] I change it for dbtunner
                likelihoods['value'] = likelihoods['value']
                minval = likelihoods['value'].min()
                maxval = likelihoods['value'].max()
                likelihoods['value'] = (likelihoods['value']-minval)/(maxval-minval)
                with self.__con:
                    for index, row in likelihoods.iterrows():
    
                        value = row['value']
                        iteration = int(row['iteration'])
                        
                        if id_obj == 1:
                            sql = "INSERT INTO NormLikelihood (iteration, "  + name + ") VALUES (" + str(iteration) + ", " + str(value) + ")"
                        else:
                            sql = "UPDATE NormLikelihood SET " + name + " = " + str(value) + " WHERE iteration = " + str(iteration)
                        cursor = self.__con.cursor()
                        cursor.execute(sql)
            except Exception, e:
                logging.error('Failed to insert normalized likelihood for: ' + name + ", error: " + str(e))
    
    def _normalize_likelihood(self, id_obj, value):
        minval = self.get_min_likelihood(id_obj)
        maxval = self.get_max_likelihood(id_obj)
        normval = (value-minval)/(maxval-minval)
        return normval
        
    def get_min_likelihood(self, id_obj):
        return self._fun_likelihood(id_obj, "MIN")

    def get_max_likelihood(self, id_obj):
        return self._fun_likelihood(id_obj, "MAX")

    def get_avg_likelihood(self, id_obj):
        return self._fun_likelihood(id_obj, "AVG")
        
    def _fun_likelihood(self, id_obj, sqlfun="MIN"):
        #TODO: we need the normalization for ALL values, to select the best
        with self.__con:
            sqlcmd = "SELECT " + sqlfun + "(value) as value FROM Likelihood WHERE id_name = " + str(id_obj)
            cursor = self.__con.cursor()
            res = cursor.fetch(sqlcmd)
            return res['value']
            
    def get_best_normalized_iteration(self, objs=None, limits=None, operators=None, index=0):
        sql = self._sql_norm_likelihoods(objs, limits, operators)
        with self.__con:
           
            logging.info(sql)            
            cursor = self.__con.cursor()
            cursor.execute(sql)
            res = pd.DataFrame(cursor.fetchall(), columns=['iteration','lik'])
            if len(res) == 0:
                logging.error("The filter has cero results")
            iteration = res['iteration'].values[index]
            
            logging.info('Best iteration:' + str(iteration))
            return iteration
    
    def _exists_output(self, iteration):
        if iteration is None:
            return False
        with self.__con:
            sqlcmd = "SELECT iteration FROM Output WHERE iteration = " + str(iteration) + " LIMIT 1"
            logging.info(sqlcmd)            
            cursor = self.__con.cursor()
            cursor.execute(sqlcmd)
            res = pd.DataFrame(cursor.fetchall(), columns=['iteration'])
            return len(res['iteration']) != 0

    def get_parameters(self, iteration):
        with self.__con:
            sqlcmd = "SELECT * FROM Parameter WHERE iteration = " + str(iteration) 
        return pd.read_sql(sqlcmd, con=self.__con)
    
    def get_best_output(self, id_comp, objs=None, limits=None, operators=None):
        iteration = self.get_best_normalized_iteration(objs, limits, operators)
        return self.get_output(id_comp, iteration)

    def get_output(self, id_comp=None, iteration = None):
        if id_comp is None and iteration is None:
            where = ""
        elif id_comp is None:
            where = " WHERE iteration = " + str(iteration)
        elif iteration is None:
            where = " WHERE id_name = " + str(id_comp)
        else:
            where = " WHERE iteration = " + str(iteration) + " AND id_name = " + str(id_comp)
            
        with self.__con:
            sqlcmd = "SELECT * FROM Output" + where
            logging.info(sqlcmd)
            sdf = pd.read_sql(sqlcmd, con=self.__con, index_col=['timestamp'])
            return sdf
            
    def get_output_join_likelihood(self, id_comp, id_obj, limit, operator):

        #sqlcmd = "SELECT * FROM Output WHERE id_name = " + str(id_comp) + " AND iteration IN (SELECT iteration FROM Likelihood WHERE id_name = " + str(id_obj) + " AND value <= " + str(threshold) + ")"
        sqlcmd = ("SELECT O.* FROM Output O WHERE O.iteration IN (SELECT iteration from Likelihood WHERE id_name = " + str(id_obj) + " AND value " + operator  + str(limit) + ")" +
                  " AND O.id_name = " + str(id_comp))
        logging.info(sqlcmd)
        with self.__con:
            sdf = pd.read_sql(sqlcmd, con=self.__con, index_col=['timestamp'])
            return sdf
    
    def _sql_norm_likelihoods(self, objs=None, limits=None, operators=None):
        #If there is no filter we order by the second column
        if objs is None:
            return "SELECT * FROM NormLikelihood ORDER BY 2 DESC"
        else: 
            sql = "SELECT iteration, "
            where = " WHERE "
            for i in range(len(objs)):
                sql = sql + objs[i] + "+"
                where = where + objs[i] + " " + operators[i] + " " + str(limits[i]) + " AND "
            sql = sql[:-1] + " AS lik FROM NormLikelihood"
            where = where[:-4] + " ORDER BY lik DESC"
            sql = sql + where
            
            return sql

    def get_output_join_likelihoods(self, id_comp, id_objs, limits, operators):
        iterationsql = self._sql_norm_likelihoods(id_objs, limits, operators)
        sqlcmd = ("SELECT O.*, L.value AS likelihood, L.id_name AS likid FROM Output O, Likelihood L WHERE O.iteration IN " + iterationsql + 
                 " AND O.iteration = L.iteration AND O.id_name = " + str(id_comp))
        
        logging.info(sqlcmd)
        with self.__con:
            sdf = pd.read_sql(sqlcmd, con=self.__con, index_col=['timestamp'])
            logging.debug('Filtering outputs by likid')
            sdf = sdf.query('likid == ' + str(id_objs[0]))
            return sdf            
            
#TODO: check that this work
    def get_parameters_join_likelihoods(self, id_objs, limits, operators):
        #nv = []
        
        sqllik1 = "SELECT L1.* FROM Likelihood L1"
        sqllik2 = ""
        for i in range(len(id_objs)):
            if i != 0:
                sqllik1 = sqllik1 + " JOIN Likelihood L" + str(i+1) + " ON L1.iteration = L" + str(i+1) + ".iteration"
            
            
            if len(id_objs) == 1:
                sqllik2 = "WHERE L" + str(i+1) + ".id_name = " + str(id_objs[i]) + " AND L" + str(i+1) + ".value " + operators[i] + str(limits[i])
            else:
                sqllik2 = sqllik2 + " AND L" + str(i+1) + ".id_name = " + str(id_objs[i]) + " AND L" + str(i+1) + ".value " + operators[i] + str(limits[i])
            
        sqllik = sqllik1 + " " + sqllik2
        #sqllik = ("SELECT L1.* FROM Likelihood L1 JOIN Likelihood L2 WHERE L1.iteration = L2.iteration"
        #          " AND l1.id_name = " + str(nv[0][0]) + " AND L1.value < " + str(nv[0][1]) + 
        #          " AND L2.id_name = " + str(nv[1][0]) + " AND L2.value < " + str(nv[1][1]))
                
        sqlcmd = ("SELECT P.* FROM Parameter P JOIN (" + sqllik + 
                  ") AS L WHERE P.iteration = L.iteration")
        
        logging.info(sqlcmd)
        with self.__con:
            sdf = pd.read_sql(sqlcmd, con=self.__con, index_col=['timestamp'])
            return sdf
            
   
    def get_measurement(self, id_name = None):
        with self.__con:
            if id_name is None:
                sqlcmd = "SELECT * FROM Measurement"
            else:
                sqlcmd = "SELECT * FROM Measurement WHERE id_name = " + str(id_name)
            sdf = pd.read_sql(sqlcmd, con=self.__con, index_col=['timestamp'])
            return sdf
    
    #TODO: in order please
    def get_name_ids(self, names):
        with self.__con:
            for i in range(len(names)):
                if i == 0:
                    sqlcmd = "SELECT id_name, %d AS sort FROM Name WHERE name = '%s'" % (i, names[i])
                else:
                    sqlcmd = sqlcmd + " UNION SELECT id_name, %d AS sort FROM Name WHERE name = '%s'" % (i, names[i])
            
            if len(names) > 1:
                sqlcmd = sqlcmd + " ORDER BY sort"
        
            cursor = self.__con.cursor()
            cursor.execute(sqlcmd)
            res = pd.DataFrame(cursor.fetchall(), columns=['id_name','sort'])
            return res['id_name'].values
            
    def get_names(self):
        with self.__con:
            sqlcmd = "SELECT * FROM Name"
            sdf = pd.read_sql(sqlcmd, con=self.__con)
            return sdf
            
    def get_index(self):
        with self.__con:
            sqlcmd = "SELECT DISTINCT timestamp FROM Output ORDER BY timestamp ASC"
            sdf = pd.read_sql(sqlcmd, con=self.__con, index_col=['timestamp'])
#TODO: include year filter in this class            
            #sdf = sdf[365:len(sdf)]
            return sdf
        
    
        
        
        

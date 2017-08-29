# -*- coding: utf-8 -*-
"""
Created on Sun Apr 02 01:25:23 2017
Responsability:
Run the ML algorithms for the Ldndc resutls
@author: ruiz-i
"""

from ldndc.datasets import Datasets
import logging
import os
import sys
from sklearn import metrics

import ldndc.config as config
from ldndc.persistence import Persistence

from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_val_score, KFold
from sklearn import ensemble
from scipy.stats import sem
import numpy as np

from sklearn import svm
from sklearn import preprocessing

import ml.imbalance as imb

from ldndc.plot import Plot
import matplotlib.pyplot as plt

def _explore_data(inputs, targets,inputnames,targetnames):
    
    fig = plt.figure()
    #plt.figure(1)
    #fig, ax = plt.subplot(nrows=targets.shape[0],ncols=inputs.shape[0])
   # plt.tight_layout()
    num = 1
    for i in range(targets.shape[0]):
        for j in range(inputs.shape[0]):
            ax = fig.add_subplot(4,5,num)
            num = num+1
            ax.scatter(inputs[:,j],targets[:,i])
            ax.set(title="", xlabel=inputnames[j]+str(max(targets[:,i])), ylabel='Uncertainty')
    
    plt.savefig('c:\\projects\\ML\\MMUA\\foo.png')
    plt.show()
        
    
def train_and_evaluate(clf, X_train, y_train):
    clf.fit(X_train, y_train)
    print "Coefficient of determination on training set:",clf.score(X_train, y_train)
     # create a k-fold cross validation iterator of k=5 folds
 #   cv = KFold(X_train.shape[0], shuffle=True,random_state=33)
 #   scores = cross_val_score(clf, X_train, y_train, cv=cv)
  #  print "Average coefficient of determination using 5-fold crossvalidation:",np.mean(scores)


def measure_performance(X, y, clf, show_accuracy=False,
                        show_classification_report=False, show_confusion_matrix=False,
                        show_r2_score=False, show_f1_score=False):
    y_pred = clf.predict(X)
    if show_accuracy:
        print "Accuracy:{0:.3f}".format(metrics.accuracy_score(y, y_pred)),"\n"
    
    if show_classification_report:
        print "Classification report"
        print metrics.classification_report(y, y_pred),"\n"
    
    if show_confusion_matrix:
        print "Confusion matrix"
        print metrics.confusion_matrix(y, y_pred),"\n"
    
    if show_r2_score:
        print "Coefficient of determination:{0:.3f}".format(metrics.r2_score(y, y_pred)),"\n"
    
if __name__ == "__main__":

    config_file = str(sys.argv[1])
    
    config.parser = config.set(config_file)
    logfile = config.parser.get('global', 'logfile')
    loglevel = config.parser.get('global', 'loglevel')
    logging.basicConfig(filename=logfile,level=loglevel)
    logging.debug("config file:"+ config_file)
    logging.info("Start!")
    try:
       # targets =  config.parser.get('global', 'targets')
        targets = [e.strip() for e in config.parser.get('global', 'targets').split(',')]
        #inputs =  config.parser.get('global', 'inputs')
        inputs = [e.strip() for e in config.parser.get('global', 'inputs').split(',')]
        
        calibrated_dbname =  config.parser.get('calibrated', 'name')
        calibrated_dbpath =  config.parser.get('calibrated', 'path')
        
        newsite_dbname =  config.parser.get('newsite', 'name')
        newsite_dbpath =  config.parser.get('newsite', 'path')
        
        calibrated_db = Persistence(calibrated_dbpath, calibrated_dbname, new=False)
        newsite_db = Persistence(newsite_dbpath, newsite_dbname, new=False)

        calibrated_data = Datasets(calibrated_db, targets, inputs)
        calibrated_data.load()
        newdata, newtarget = imb.Imbalance().avgsampling(calibrated_data.data.values, 
        calibrated_data.target.values)
        
        inputnames = list(calibrated_data.data)
        targetnames = list(calibrated_data.target)
        #newdata, newtarget = imb.Imbalance().oversampling(data.data.values, data.target.values)
        #_explore_data(newdata, newtarget,inputnames,targetnames)
        
        newsite_data = Datasets(newsite_db, targets, inputs)
        newsite_data.load(training=False)     
        newsitedata, nulltarget = imb.Imbalance().avgsampling(newsite_data.data.values,
        newsite_data.target.values)
        
        #TODO: this data has to be categorized in imbalance, probably the normalization should be done using also the calibrated inputs.
        
        X_train, X_test, y_train, y_test = train_test_split(
        newdata, newtarget, test_size=0.7, random_state=42)

        # Standardize the features
        scalerX = preprocessing.StandardScaler().fit(X_train)
        scalery = preprocessing.StandardScaler().fit(y_train)
        
        X_train = scalerX.transform(X_train)
        X_test = scalerX.transform(X_test)
        y_train = scalery.transform(y_train)
        y_test = scalery.transform(y_test)
        
        clf_svr_linear = svm.SVR(kernel='linear')
        clf_svr_poly = svm.SVR(kernel='poly', degree=3, coef0=1, C=5)
        clf_svr_rbf = svm.SVR(kernel='rbf')
      
        train_and_evaluate(clf_svr_linear,X_train,y_train)
        train_and_evaluate(clf_svr_poly,X_train,y_train)
        train_and_evaluate(clf_svr_rbf,X_train,y_train)
      
        print "SVM linear performance:"
        measure_performance(X_test, y_test, clf_svr_linear,show_r2_score=True,show_f1_score=True)
        print "SVM polynomial performance:"
        measure_performance(X_test, y_test, clf_svr_poly,show_r2_score=True,show_f1_score=True)
        print "SVM rbf performance:"
        measure_performance(X_test, y_test, clf_svr_rbf,show_r2_score=True,show_f1_score=True)

        X_predict = scalerX.transform(newsitedata)
        
        y_prediction_poly = clf_svr_poly.predict(X_predict)
        new_targets_poly = scalery.inverse_transform(y_prediction_poly)
        y_prediction_linear = clf_svr_linear.predict(X_predict)
        new_targets_linear = scalery.inverse_transform(y_prediction_linear)
        y_prediction_rbf = clf_svr_rbf.predict(X_predict)
        new_targets_rbf = scalery.inverse_transform(y_prediction_rbf)
        
        #TODO: plot new site with outputs, predicted uncertainties and measurements
         
    except Exception, e:
        logging.error(str(e))
    finally:
        calibrated_db.close()
        newsite_db.close()
   
   
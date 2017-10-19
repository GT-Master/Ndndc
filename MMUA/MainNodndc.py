# -*- coding: utf-8 -*-
"""
Created on Thu Aug 24 17:15:26 2017
Responsability:
This class is the main program to create a Nodndc model

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
from modules import nodndc as model

import ml.imbalance as imb

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
    logging.info("Start creating the Nodndc!")
    
    ndndc = model.Nodndc()
    
    tr = ndndc.target
    dt = ndndc.data
    
    newdata, newtarget = imb.Imbalance().avgsampling(ndndc.data.values, 
    ndndc.target.values)
    
    inputnames = list(ndndc.data)
    targetnames = list(ndndc.target)
    
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
    train_and_evaluate(clf_svr_linear,X_train,y_train)
    print "SVM linear performance:"
    measure_performance(X_test, y_test, clf_svr_linear,show_r2_score=True,show_f1_score=True)
    
    clf_svr_poly = svm.SVR(kernel='poly', degree=3, coef0=1, C=5)
    train_and_evaluate(clf_svr_poly,X_train,y_train)
    print "SVM polynomial performance:"
    measure_performance(X_test, y_test, clf_svr_poly,show_r2_score=True,show_f1_score=True)
    
    clf_svr_rbf = svm.SVR(kernel='rbf', gamma=50, C=1)
    train_and_evaluate(clf_svr_rbf,X_train,y_train)
    print "SVM rbf performance:"
    measure_performance(X_test, y_test, clf_svr_rbf,show_r2_score=True,show_f1_score=True)


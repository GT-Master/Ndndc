# -*- coding: utf-8 -*-
"""
Created on Sat Apr 15 14:44:30 2017

@author: ruiz-i
"""
import numpy as np

class Imbalance(object):
    def __init__(self):
        pass
   
    def _avgcatlen(self, target_cat):
        ''' Calculate the average number of samples per category'''
        avg_num = 0
        lens =[]
        for cat in np.unique(target_cat):
            idx = np.where(target_cat == cat)
            lens.append(len(idx[0]))
            print "number of category " + str(cat) + ":" + str(len(idx[0]))
        avg_num = np.mean(lens)
        return avg_num

    def oversampling(self, data, target):
        target_cat = self._categorize(target)
        avg_len = self._avgcatlen(target_cat)
        #data = np.apply_along_axis(self._categorize, 0, data)

        unq, unq_idx = np.unique(target_cat[:, -1], return_inverse=True)
        unq_cnt = np.bincount(unq_idx)
        cnt = np.max(unq_cnt)
        out = np.empty((cnt*len(unq) - len(target_cat),) + target_cat.shape[1:], target_cat.dtype)
        newdata = np.empty((cnt*len(unq) - len(data),) + data.shape[1:], data.dtype)
        newtarget = np.empty((cnt*len(unq) - len(target),) + target.shape[1:], target.dtype)
        slices = np.concatenate(([0], np.cumsum(cnt - unq_cnt)))
        for j in xrange(len(unq)):
            indices = np.random.choice(np.where(unq_idx==j)[0], cnt - unq_cnt[j])
            out[slices[j]:slices[j+1]] = target_cat[indices]
            newdata[slices[j]:slices[j+1]] = data[indices]
            newtarget[slices[j]:slices[j+1]] = target[indices]
        out = np.vstack((target_cat, out))
        newdata = np.vstack((data, newdata))
        newtarget = np.vstack((target, newtarget))
        for cat in np.unique(out):
            idx = np.where(out == cat)
            print "number of category " + str(cat) + ":" + str(len(idx[0]))
        #return newdata, out
        return newdata, newtarget
            
    def avgsampling(self, data, target):
        target_cat = self._categorize(target)
        #avg_len = self._avgcatlen(target_cat)
        data = np.apply_along_axis(self._categorize, 0, data)

        unq, unq_idx = np.unique(target_cat[:, -1], return_inverse=True)
        unq_cnt = np.bincount(unq_idx)
        avg_cnt = int(np.mean(unq_cnt))
        #cnt = np.min(unq_cnt)
        out = np.empty((avg_cnt*len(unq),) + target_cat.shape[1:], target_cat.dtype)
        newdata = np.empty((avg_cnt*len(unq),) + data.shape[1:], data.dtype)
        newtarget = np.empty((avg_cnt*len(unq),) + target.shape[1:], target.dtype)
       
        for j in xrange(len(unq)):
            #TODO: use random indices to remove rows from the arrays
           # if unq_cnt[j] > avg_cnt:
            indices = np.random.choice(np.where(unq_idx==j)[0], avg_cnt)
                #out = np.delete(target_cat, indices)
            #else:
             #   indices = np.random.choice(np.where(unq_idx==j)[0], avg_cnt - unq_cnt[j])
            out[avg_cnt*j:avg_cnt*j+avg_cnt] = target_cat[indices]
            newdata[avg_cnt*j:avg_cnt*j+avg_cnt] = data[indices]
            newtarget[avg_cnt*j:avg_cnt*j+avg_cnt] = target[indices]
        #out = np.vstack((target_cat, out))
 #       newdata = np.vstack((data, newdata))
 #       newtarget = np.vstack((target, newtarget))
        newtarget = self._categorize(newtarget)
        for cat in np.unique(out):
            idx = np.where(out == cat)
            print "number of category " + str(cat) + ":" + str(len(idx[0]))
        #return newdata, out
        return newdata, newtarget
    
    def _categorize(self,serie, n=1.0):
        '''Normalize the serie values and divide them in ten categories
        
        Arguments
        - Serie: the values to normalize
        - n: 1 for ten categories, 2 for five, 3 for three
        '''
        maxval = serie.max()
        minval = serie.min()
        #serie = (((serie-minval)/(maxval-minval))/n).round(1)
        serie = ((serie-minval)/(maxval-minval)).round(1)
        
        return serie

    
    
#!/usr/bin/python2
# -*- coding: utf-8 -*-

from pyspark.sql.functions import col
import json
import math
import cPickle as pickle


def loadDataJson(test_path='', usr_path='', bus_path=''):
    # load test df
    stars = pickle.load(open(test_path, 'rb'))
    testDF = sc.parallelize([(list(k)[0], list(k)[1]) + (v['stars'], v['rev_id']) for k, v in stars.items()]) \
               .toDF(['bus_id', 'usr_id', 'label', 'rev_id'])
    testDF = testDF.select('bus_id', 'usr_id', 'label')
    # print "this is testDF"

    # load usr df
    with open(usr_path, 'r') as f:
        usr = json.load(f)
    usrDF = sc.parallelize([ k, v['votes'], v['review_count'], v['average_stars']] for k, v in usr.items()) \
              .toDF(['usr_id', 'votes','urew_no', 'uavg_stars'])
    usrDF = usrDF.select('usr_id','urew_no',(col('urew_no')*col('uavg_stars')).alias('usrtemp'))
    #print "this is usrDF"
    #usrDF.show()

    # load bus df
    with open(bus_path, 'r') as f:
        bus = json.load(f)
    busDF = sc.parallelize([ k, v['city'], v['stars'],v['review_count'],v['name'],v['categories'] ] for k, v in bus.items()) \
              .toDF(['bus_id','city','bavg_stars','brew_no','name','cate'])
    busDF = busDF.select('bus_id','brew_no',(col('brew_no')*col('bavg_stars')).alias('bustemp'))
    #print "this is busDF"
    # busDF.show()

    return testDF,usrDF,busDF

def calculateRMSE(testDF,usrDF,busDF):
    # real calculation
    baseDF = testDF.join(usrDF, testDF.usr_id == usrDF.usr_id).drop(usrDF.usr_id) \
               .join(busDF, testDF.bus_id == busDF.bus_id).drop(busDF.bus_id)

    baseDF = baseDF.select('*',((col('usrtemp')+col('bustemp'))/(col('urew_no')+col('brew_no'))).alias('baserating'))
    #baseDF.show()
    rmseDF = baseDF.select('label','baserating',((col('label') - col('baserating'))**2).alias('mes'))
    errors = rmseDF.rdd.map(lambda x: x.mes).collect()
    RMSE = math.sqrt(sum(errors)/len(errors))
    return RMSE
    
if __name__ == '__main__':

    test_path = './stars.pk'
    usr_path = './trainingUser.json'
    bus_path = './trainingBusiness.json'

    testDF,usrDF,busDF = loadDataJson(test_path=test_path, usr_path=usr_path, bus_path=bus_path)
    RMSE = calculateRMSE(testDF,usrDF,busDF)
    print 'RMSE: %.8f' % RMSE

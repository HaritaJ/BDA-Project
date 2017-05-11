#!/usr/bin/env python

# scrip to Extract Information using SparkSQL Query

# import numpy as np

# -*- coding: utf-8 -*-
import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SQLContext,Row
import re
import csv
from csv import reader
def rdd_filter(x):
  y=x
  x=x.strip("(")
  x = x.strip(")")
  x = x.split(",")
  try:
    lat =float(x[0])
    lon =float(x[1])
    if ((40.746217<= lat <= 40.75106140) and (-73.992448<= lon <= -73.984208)):
      return True
    else:
      return False
  except ValueError:
      return False


if __name__ == "__main__":
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    rdd1 = sc.textFile(sys.argv[1],1)
    #header = rdd1.first()
    #data = rdd1.filter(lambda x: x != header)
    rdd2 = rdd1.mapPartitions(lambda x:reader(x)).map(lambda x:(x[0],x[1],x[6],x[7],x[10],x[13],x[15],x[21],x[22],x[23],(rdd_filter(x[23])))).filter(lambda x: x[10]==True).map(lambda x:(x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9]))
    myDF1=sqlContext.createDataFrame(rdd2,('CMPLNT_NO','CMPLNT_FR_DT','KY_CD','OFNS_DESC','CRM_ATPT_CPTD_CD','BORO_NM','LOC_OF_OCCUR_DESC','LATITUDE','LONGITUDE','LAT-LONG'))
    myDF1.createOrReplaceTempView("df1_view")
    finalRDD = sqlContext.sql(
         "SELECT * FROM df1_view"
        )   
    finalRDD.repartition(1).write.csv("PennStation.csv", sep=',', header=True)
    sc.stop()
#-73.992448,40.746217,-73.984208,40.75106140.74

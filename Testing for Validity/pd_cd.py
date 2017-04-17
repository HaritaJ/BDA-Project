from __future__ import print_function
# -*- coding: utf-8 -*-
import sys
from operator import add
from pyspark import SparkContext
import re
import csv
from csv import reader
def rdd_check(x):
   if x is "" or x is " ":
      return "/------/","       NULL","         OTHER","        NULL"
   else:
      try:
        val=int(x)
        return x,"      NUMBER","       PD_CD","        VALID"
      except:
        return x,"      OTHER","        OTHER","        INVALID"
 
 
if __name__ == "__main__":
    sc = SparkContext()
    rdd1 = sc.textFile(sys.argv[1],1)
    rdd3 = rdd1.mapPartitions( lambda x: reader(x)).map(lambda x: x[8])
    header = rdd3.first()
    data = rdd3.filter(lambda x: x != header)
    rdd4 = data.map(lambda x:'%s\t%s\t%s\t%s'%(rdd_check(x)))
    rdd4.saveAsTextFile("pd_cd.out")
    sc.stop()
 

from __future__ import print_function
# -*- coding: utf-8 -*-
import sys
from operator import add
from pyspark import SparkContext
import re
import csv
from csv import reader
def rdd_filter(x):
    pattern=re.compile(r"^\(([-\d.]+), ([-\d.]+)\)$")
    match=pattern.match(x)
    if match:
       y = x
       x=x.strip("(")
       x = x.strip(")")
       x = x.split(",")
       lat = float(x[0])
       lon = float(x[1])
       if ((40.4774 <= lat <= 40.9176) and (-74.2591 <= lon <= -73.7002)):
                return y,"LOCATION","LAT-LONG","VALID"
       else:
                return y,"Lat-LongInval","Other","Invalid"
    else:
       return x,"NULL","OTHER","INVALID"
 
 
if __name__ == "__main__":
    sc = SparkContext()
    rdd1 = sc.textFile(sys.argv[1],1)
    #rdd3 = rdd1.mapPartitions( lambda x: reader(x)).map(lambda x: x[23])
    header = rdd1.first()
    data = rdd1.filter(lambda x: x != header)
    rdd2 = data.mapPartitions(lambda x:reader(x)).map(lambda x:'%s\t%s\t%s\t%s'%(rdd_filter(x[23])))
    rdd2.saveAsTextFile('4.out')
    sc.stop()

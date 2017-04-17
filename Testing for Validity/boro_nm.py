from __future__ import print_function
# -*- coding: utf-8 -*-
import sys
from operator import add
from pyspark import SparkContext
import csv
from csv import reader

def rdd_city(x):
    if x is "" or x is " " or x is None:
       return "-","null","OTHER","NULL"
    else:
       if x=="MANHATTAN":
          return x,"TEXT","BURROW NAME","VALID"
       elif x=="BRONX":
          return x,"TEXT","BURROW NAME","VALID"
       elif x=="QUEENS":
          return x,"TEXT","BURROW NAME","VALID"
       elif x=="STATEN ISLAND":
          return x,"TEXT","BURROW NAME","VALID"
       elif x=="BROOKLYN":
          return x,"TEXT","BURROW NAME","VALID"
       else:
          return x,"OTHER","OTHER","INVALID"


if __name__ == "__main__":
    sc=SparkContext()
    rdd1 = sc.textFile(sys.argv[1],1)
    rdd1 = rdd1.mapPartitions( lambda x: reader(x)).map(lambda x: (x[13]))
    header=rdd1.first()
    rdd2=rdd1.filter(lambda x: x!= header)
    rdd3=rdd2.map(lambda x:'%s\t%s\t%s\t%s'% (rdd_city(x)))
    rdd3.saveAsTextFile("boro_nm.out")
    sc.stop()

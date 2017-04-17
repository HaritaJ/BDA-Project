from __future__ import print_function
from pyspark import SparkContext
from csv import reader
import sys
import re
import datetime
def check(x):
        if x is "NULL" or x is "":
                return "  NULL  ","NULL","  OTHER ","NULL"
        else :
		return x,"TEXT","INDICATOR","VALID"
if __name__ == "__main__":
   sc = SparkContext()
   date = sc.textFile(sys.argv[1], 1)
   header = date.first() #extract header
   date = date.filter(lambda x : x!= header)
   date = date.mapPartitions(lambda x: reader(x)) \
	.map(lambda x: '%s\t%s\t%s\t%s'%(check(x[10])))
   date.saveAsTextFile('crimeattempt.out')                          

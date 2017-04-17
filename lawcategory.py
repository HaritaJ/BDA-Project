from __future__ import print_function
from pyspark import SparkContext
from csv import reader
import sys
import re
import datetime
def validity(x):
        if x is "NULL" or x is "":
                return "  NULL  ","NULL","  OTHER ","NULL"
        else :
                return x,"TEXT","CATEGORY","VALID"
if __name__ == "__main__":
   sc = SparkContext()
   category = sc.textFile(sys.argv[1], 1)
   header = category.first() #extract header
   category = category.filter(lambda x : x!= header)
   category = category.mapPartitions(lambda x: reader(x)).map(lambda x: '%s\t%s\t%s\t%s'%(validity(x[11])))
   category.saveAsTextFile('lawcategory.out')                                                                                                                                                                                                                              

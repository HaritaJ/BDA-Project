from __future__ import print_function
from pyspark import SparkContext
from csv import reader
import sys
import re
import datetime
def check(x):
        if x is "" or x is " ":
                return "","NULL","OTHER","NULL"
        else :
           y=x
           x=x.split("/")
           try:
               year=int(x[2])
               month=int(x[0])
               day= int(x[1])
               if year >=2006 and year <=2016 :
                   try:
                       newDate= datetime.datetime(year,month,day)
                       return y,"DateTime","Date of the event","VALID"
                   except :
                       return y,"OTHER","OTHER","INVALID"
               else :
                   return y,"DateTime","Date of the event","INVALID"
           except:
               return y,"OTHER","OTHER","INVALID"
if __name__ == "__main__":
   sc = SparkContext()
   date = sc.textFile(sys.argv[1], 1)
   header = date.first() #extract header
   date = date.filter(lambda x : x!= header)
   date = date.mapPartitions(lambda x: reader(x)) \
	.map(lambda x: '%s\t%s\t%s\t%s'%(check(x[5])))
   date.saveAsTextFile('reportdate.out')                                       

from __future__ import print_function
from pyspark import SparkContext
from csv import reader
import sys
import re
import datetime
import time 
def validity(x):
	if x is "" or x is " ":
		return "--:--:--","NULL","       OTHER        ","NULL"
	else :
           y=x
           x=x.split(":")
           try:
               seconds=int(x[2])
               hour=int(x[0])
               minute= int(x[1])
               try:  
                   newTime= datetime.datetime.strptime(y,"%H:%M:%S")
                   return y,"Time","Time of the event","VALID"
               except  ValueError :
                    return y,"OTHER","     OTHER      ","INVALID"
               
           except ValueError:
               return y,"OTHER","OTHER","INVALID"
if __name__ == "__main__":
   sc = SparkContext()
   date = sc.textFile(sys.argv[1], 1)
   header = date.first() #extract header
   date = date.filter(lambda x : x!= header)
   date = date.mapPartitions(lambda x: reader(x)) \
	.map(lambda x: '%s\t%s\t%s\t%s'%(validity(x[2])))
   date.saveAsTextFile('timestart.out')

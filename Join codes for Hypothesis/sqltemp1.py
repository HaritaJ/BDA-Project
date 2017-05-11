
#!/usr/bin/env python

# scrip to Extract Information using SparkSQL Query

# import numpy as np

import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import csv
from csv import reader
import re
from datetime import datetime
from dateutil.parser import parse

def myValid(date):
    try:
    	date = parse(date)
    	date = date.strftime("%m-%d-%y")
    	return str(date)
    except ValueError:
        return "error"




##Code Ends for Checking of Valid Agency

if __name__ == "__main__":
    #if len(sys.argv) != 3:
     #   print("Usage: cleaning_date.py <file>")
      #  exit(-1)
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    csvfile1 = sc.textFile(sys.argv[1], 1)
    csvfile2 = sc.textFile(sys.argv[2],1)
    ## Creating Agency RDD and Filtering only Valid Values
    df1 = csvfile1.mapPartitions(lambda x: csv.reader(x)).map(lambda x:(x[0],(myValid(x[1])),x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9]))
    df2 = csvfile2.mapPartitions(lambda x: csv.reader(x)).map(lambda x:((myValid(x[2])),x[3],x[13]))
   # edf1 = sc.parallelize(df1.take(10))

    #edf2 = sc.parallelize(df2.take(10))
    #edf1.saveAsTextFile('out.out')

    myDF1 = sqlContext.createDataFrame(df1,('Complaint_num','CMPLNT_FR_DT','KY_CD','OFNS_DESC','Crm_Atpt_Cptd_Cd','BORO_NM','Loc Of Occur Desc','Latitude','Longitude','Lat-Long'))
    myDF2 = sqlContext.createDataFrame(df2,('Yearmoda','Temp','Precp'))
    #emyDF1 = sc.parallelize(myDF2.take(10))
    #emyDF1.saveAsTextFile('out.out')

    myDF1.createOrReplaceTempView("df1_view")
    myDF2.createOrReplaceTempView("df2_view")


    finalRDD = sqlContext.sql(
        "SELECT * fROM df1_view INNER JOIN df2_view ON df1_view.CMPLNT_FR_DT=df2_view.Yearmoda"
        )

    finalRDD.repartition(1).write.csv("PrecpPennStation.csv", sep=',', header=True)
    sc.stop()

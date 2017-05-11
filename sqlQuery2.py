
#!/usr/bin/env python

# scrip to Extract Information using SparkSQL Query

# import numpy as np

import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import csv
import re
from datetime import datetime
from dateutil.parser import parse

def myValid(date):
    y = date
    try:
        date = parse(date)
        date = date.strftime("%m-%d-%Y")
        return str(date)
    except ValueError:
        return(y)
##Code Ends for Checking of Valid Agency
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: cleaning_date.py <file>")
        exit(-1)
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    csvfile1 = sc.textFile(sys.argv[1], 1)
    csvfile2 = sc.textFile(sys.argv[2],1)
    ## Creating Agency RDD and Filtering only Valid Values
    df1 = csvfile1.mapPartitions(lambda x: csv.reader(x)).map(lambda x:((myValid(x[1])),x[2],x[3],x[4],x[5],x[6],x[7],x[8]))
    df2 = csvfile2.mapPartitions(lambda x: csv.reader(x)).map(lambda x:((myValid(x[2])),x[0],x[3],x[4]))

    myDF1 = sqlContext.createDataFrame(df1,('CMPLNT_FR_DT','KY_CD','OFNS_DESC','CMPLT_ATTMPT','LOC_OF_OCCUR_DESC','BURROW','Latitude','Longitude'))
    myDF2 = sqlContext.createDataFrame(df2,('Yearmoda','Stn---','Temp','Dewp'))
         
          
    myDF1.createOrReplaceTempView("df1_view")
    myDF2.createOrReplaceTempView("df2_view")

             
    finalRDD = sqlContext.sql(
        "SELECT * fROM df1_view dc JOIN df2_view dw ON dc.CMPLNT_FR_DT=dw.Yearmoda"
        )
    
    finalRDD.repartition(1).write.csv("sqlQuery1.csv", sep=',', header=True)
    sc.stop() 

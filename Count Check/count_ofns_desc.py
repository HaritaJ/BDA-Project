from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines= lines.mapPartitions( lambda x:reader(x))
    header=lines.first()
    lines=lines.filter(lambda x:x!=header)
    counts=lines.map(lambda x:((x[7]),1))
    counts=counts.reduceByKey( lambda a,b :a+b)
    counts=counts.sortByKey(True)
    df=counts.map(lambda r:"\t".join([str(c) for c in r]))\
        .collect()
    out = sc.parallelize(df)
    out.saveAsTextFile("count_ofns_desc.out")
    sc.stop()

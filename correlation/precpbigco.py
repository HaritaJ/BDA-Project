import sys
import pandas as pd
import csv
from csv import reader


df = pd.read_csv('PrecpBig.csv')
x=df['OFNS_DESC'].corr(df['Temp'],method='spearman')
print (x)

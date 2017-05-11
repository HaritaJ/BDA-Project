import sys
import pandas as pd
import csv
from csv import reader


df = pd.read_csv('Big5.csv')
x=df['Ofns_Desc'].corr(df['Contri_Fact_1'],method='spearman')
print (x)

import sys
import pandas as pd
import csv
from csv import reader


df = pd.read_csv('Big2.csv')
x=df['Boro_Nm'].corr(df['Cmplnt_Fr_Dt'],method='spearman')
print (x)

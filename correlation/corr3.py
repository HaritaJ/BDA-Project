import sys
import pandas as pd
import csv
from csv import reader


df = pd.read_csv('Big3.csv')
x=df['precp'].corr(df['Cmplt_Fr_Dt'],method='spearman')
print (x)

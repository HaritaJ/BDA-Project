import pandas as pd
import os
import numpy as np
import matplotlib.pyplot as plt

crime=pd.read_csv('NYPD_Complaint_Data_Historic.csv')
is_burrow=crime['BORO_NM']=="BRONX"
high_crime=crime[is_burrow]
h_crime=high_crime['OFNS_DESC'].value_counts()
h_crime.plot(kind='bar')
plt.show()

import pandas as pd
import os
import numpy as np
import matplotlib.pyplot as plt

crime=pd.read_csv('NYPD_Complaint_Data_Historic.csv')
is_crime=crime['BORO_NM'].value_counts()
is_crime.plot(kind='bar')
plt.show()

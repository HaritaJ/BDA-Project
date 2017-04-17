import pandas as pd
import os
import numpy as np
import matplotlib.pyplot as plt

crime=pd.read_csv('NYPD_Complaint_Data_Historic.csv')
is_crime=crime['KY_CD'].value_counts()
is_crime.plot(kind='bar')
plt.show()

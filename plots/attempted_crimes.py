import pandas as pd
import os
import numpy as np
import matplotlib.pyplot as plt

crime=pd.read_csv('NYPD_Complaint_Data_Historic.csv')
is_attmp=crime['CRM_ATPT_CPTD_CD']=="ATTEMPTED"
is_burrow=crime[is_attmp]
ans=is_burrow['BORO_NM'].value_counts()
ans.plot(kind="bar")
plt.show()

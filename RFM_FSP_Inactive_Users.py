#!/usr/bin/env python
# coding: utf-8

# In[3]:


import numpy as np;
import pandas as pd;
import csv, sqlite3
from pyhive import presto
import pymysql;

inact_drivers_rfm_table = pd.read_csv("bi_inactive_driver_rfm_data.csv")
print(len(inact_drivers_rfm_table))
inact_drivers_rfm_table.head()


# In[4]:


quantiles = inact_drivers_rfm_table.quantile(q=[0.25,0.5,0.75])
quantiles = quantiles.to_dict()
inact_drivers_rfm_table['r_quartile'] = 0

def RScore(x,d):
    if x[d] <= 60:
        return 1
    elif x[d] <= 90:
        return 2
    elif x[d] <= 120: 
        return 3
    else:
        return 4
    
def FMScore(x,p,d):
    if x <= d[p][0.25]:
        return 4
    elif x <= d[p][0.50]:
        return 3
    elif x <= d[p][0.75]: 
        return 2
    else:
        return 1

inact_drivers_rfm_table.head()


# In[5]:


segmented_rfm = inact_drivers_rfm_table
inact_drivers_rfm_table['r_quartile'].loc[(inact_drivers_rfm_table['recency'] <= 60) & (inact_drivers_rfm_table['recency'] >= 30)] = 1
inact_drivers_rfm_table['r_quartile'].loc[(inact_drivers_rfm_table['recency'] <= 90) & (inact_drivers_rfm_table['recency'] > 60)] = 2
inact_drivers_rfm_table['r_quartile'].loc[(inact_drivers_rfm_table['recency'] <= 120) & (inact_drivers_rfm_table['recency'] > 90)] = 3
inact_drivers_rfm_table['r_quartile'].loc[(inact_drivers_rfm_table['recency'] >120)] = 4

segmented_rfm['f_quartile'] = segmented_rfm['frequency'].apply(FMScore, args=('frequency',quantiles,))
segmented_rfm['m_quartile'] = segmented_rfm['contribution'].apply(FMScore, args=('contribution',quantiles,))
segmented_rfm['RFMScore'] = segmented_rfm.r_quartile.map(str) + segmented_rfm.f_quartile.map(str) + segmented_rfm.m_quartile.map(str)
print(segmented_rfm.head())
segmented_rfm.groupby('f_quartile')['frequency'].describe()
segmented_rfm.groupby('r_quartile')['recency'].describe()


# In[6]:


seg = {'111' : 'POWER',
'112' : 'CORE',
'211' : 'CORE',
'212' : 'CORE',
'312' : 'CORE',
'311' : 'CORE',
'113' : 'PROMISE',
'314' : 'PROMISE',
'313' : 'PROMISE',
'114' : 'PROMISE',
'123' : 'PROMISE',
'324' : 'PROMISE',
'224' : 'PROMISE',
'124' : 'PROMISE',
'223' : 'PROMISE',
'424' : 'PROMISE',
'413' : 'PROMISE',
'414' : 'PROMISE',
'214' : 'PROMISE',
'213' : 'PROMISE',
'323' : 'PROMISE',
'332' : 'LOYAL',
'342' : 'LOYAL',
'321' : 'LOYAL',
'331' : 'LOYAL',
'141' : 'LOYAL',
'241' : 'LOYAL',
'421' : 'LOYAL',
'131' : 'LOYAL',
'231' : 'LOYAL',
'221' : 'LOYAL',
'441' : 'LOYAL',
'121' : 'LOYAL',
'431' : 'LOYAL',
'132' : 'LOYAL',
'222' : 'LOYAL',
'122' : 'LOYAL',
'232' : 'LOYAL',
'242' : 'LOYAL',
'142' : 'LOYAL',
'411' : 'LOYAL',
'341' : 'LOYAL',
'322' : 'LOYAL',
'412' : 'IDLE',
'442' : 'IDLE',
'423' : 'IDLE',
'433' : 'IDLE',
'422' : 'IDLE',
'432' : 'IDLE',
'434' : 'IDLE',
'444' : 'IDLE',
'443' : 'IDLE',
'343' : 'LOW',
'144' : 'LOW',
'344' : 'LOW',
'133' : 'LOW',
'244' : 'LOW',
'334' : 'LOW',
'134' : 'LOW',
'234' : 'LOW',
'243' : 'LOW',
'143' : 'LOW',
'233' : 'LOW',
'333' : 'LOW'}

def rfm_switch(x):
    z= seg[x]
    return z

segmented_rfm['RFM_segment'] = segmented_rfm['RFMScore'].apply(rfm_switch)


# In[7]:


segmented_rfm['segment_type']="inactive"
seg_test=segmented_rfm[['id_driver','RFM_segment','RFMScore','r_quartile','f_quartile','m_quartile','recency','frequency','contribution','segment_type','updated_at']]
seg_test.head()


# In[30]:


## Inactive Passengers

inact_passengers_rfm_table = pd.read_csv("bi_inactive_passenger_rfm_data.csv")
print(len(inact_passengers_rfm_table))
inact_passengers_rfm_table.head()


# In[31]:


len(inact_passengers_rfm_table)


# In[32]:


quantiles = inact_passengers_rfm_table.quantile(q=[0.25,0.5,0.75])
quantiles = quantiles.to_dict()

#quantiles_r = inact_passengers_rfm_table.quantile(q=[0.33,0.66])
#quantiles_r = quantiles_r.to_dict()


# In[33]:


inact_passengers_rfm_table['r_quartile'] = 0


# In[34]:


#fixed Recency figures
#Quartile Frequency &  

def RScore(x,d):
    if x[d] <= 60:
        return 1
    elif x[d] <= 90:
        return 2
    elif x[d] <= 120: 
        return 3
    else:
        return 4
    
def FMScore(x,p,d):
    if x <= d[p][0.25]:
        return 4
    elif x <= d[p][0.50]:
        return 3
    elif x <= d[p][0.75]: 
        return 2
    else:
        return 1


# In[35]:


inact_passengers_rfm_table.head()


# In[36]:


segmented_rfm = inact_passengers_rfm_table


# In[37]:


inact_passengers_rfm_table['r_quartile'].loc[(inact_passengers_rfm_table['recency'] <= 60) & (inact_passengers_rfm_table['recency'] >= 30)] = 1
inact_passengers_rfm_table['r_quartile'].loc[(inact_passengers_rfm_table['recency'] <= 90) & (inact_passengers_rfm_table['recency'] > 60)] = 2
inact_passengers_rfm_table['r_quartile'].loc[(inact_passengers_rfm_table['recency'] <= 120) & (inact_passengers_rfm_table['recency'] > 90)] = 3
inact_passengers_rfm_table['r_quartile'].loc[(inact_passengers_rfm_table['recency'] >120)] = 4


# In[38]:


segmented_rfm['f_quartile'] = segmented_rfm['frequency'].apply(FMScore, args=('frequency',quantiles,))
segmented_rfm['m_quartile'] = segmented_rfm['monetary_value'].apply(FMScore, args=('monetary_value',quantiles,))
segmented_rfm.head()


# In[39]:


segmented_rfm['RFMScore'] = segmented_rfm.r_quartile.map(str) + segmented_rfm.f_quartile.map(str) + segmented_rfm.m_quartile.map(str)
segmented_rfm.head()


# In[40]:


segmented_rfm.groupby('f_quartile')['frequency'].describe()


# In[41]:


segmented_rfm.groupby('r_quartile')['recency'].describe()


# In[42]:


seg = {'111' : 'DORMANT HIGH',
'112' : 'DORMANT HIGH',
'113' : 'DORMANT HIGH',
'114' : 'DORMANT HIGH',
'123' : 'DORMANT HIGH',
'124' : 'DORMANT HIGH',
'121' : 'DORMANT HIGH',
'122' : 'DORMANT HIGH',
'141' : 'DORMANT LOW',
'131' : 'DORMANT LOW',
'132' : 'DORMANT LOW',
'142' : 'DORMANT LOW',
'144' : 'DORMANT LOW',
'133' : 'DORMANT LOW',
'134' : 'DORMANT LOW',
'143' : 'DORMANT LOW',
'211' : 'CHURNED HIGH',
'314' : 'CHURNED HIGH',
'313' : 'CHURNED HIGH',
'212' : 'CHURNED HIGH',
'214' : 'CHURNED HIGH',
'213' : 'CHURNED HIGH',
'312' : 'CHURNED HIGH',
'324' : 'CHURNED HIGH',
'224' : 'CHURNED HIGH',
'223' : 'CHURNED HIGH',
'221' : 'CHURNED HIGH',
'222' : 'CHURNED HIGH',
'323' : 'CHURNED HIGH',
'321' : 'CHURNED HIGH',
'311' : 'CHURNED HIGH',
'322' : 'CHURNED HIGH',
'241' : 'CHURNED LOW',
'231' : 'CHURNED LOW',
'232' : 'CHURNED LOW',
'242' : 'CHURNED LOW',
'333' : 'CHURNED LOW',
'341' : 'CHURNED LOW',
'332' : 'CHURNED LOW',
'342' : 'CHURNED LOW',
'331' : 'CHURNED LOW',
'343' : 'CHURNED LOW',
'344' : 'CHURNED LOW',
'244' : 'CHURNED LOW',
'334' : 'CHURNED LOW',
'234' : 'CHURNED LOW',
'243' : 'CHURNED LOW',
'233' : 'CHURNED LOW',
'421' : 'LOST HIGH',
'441' : 'LOST HIGH',
'431' : 'LOST HIGH',
'411' : 'LOST HIGH',
'412' : 'LOST HIGH',
'422' : 'LOST HIGH',
'442' : 'LOST HIGH',
'432' : 'LOST HIGH',
'424' : 'LOST LOW',
'423' : 'LOST LOW',
'433' : 'LOST LOW',
'443' : 'LOST LOW',
'434' : 'LOST LOW',
'444' : 'LOST LOW',
'413' : 'LOST LOW',
'414' : 'LOST LOW'
}

def rfm_switch(x):
    z= seg[x]
    return z

segmented_rfm['RFM_segment'] = segmented_rfm['RFMScore'].apply(rfm_switch)


# In[44]:


segmented_rfm['segment_type']="inactive"
seg_test=segmented_rfm[['id_passenger','RFM_segment','RFMScore','r_quartile','f_quartile','m_quartile','recency','frequency','monetary_value','segment_type','updated_at']]
seg_test.head()


# In[45]:


segmented_rfm.groupby('RFMScore').count()


# In[46]:


segmented_rfm.groupby('RFMScore').median()


# In[ ]:





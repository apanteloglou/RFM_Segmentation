#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np;
import pandas as pd;
import csv, sqlite3
from pyhive import presto
import pymysql


# In[2]:


## Example of code that is used to parse data from DB - not useful here
# cnxn = presto.connect(host='presto-pe.taxibeat.com',port=8080)
# cursor = cnxn.cursor();
# cnxn.close()
# sql = "select * from bi_driver_rfm_data"
# drivers_rfm_table = pd.read_sql_query(sql,cnxn,parse_dates=["updated_at"])


# In[9]:



# Active Drivers data import
drivers_rfm_table = pd.read_csv("bi_driver_rfm_data.csv")
len(drivers_rfm_table)


# In[10]:


quantiles = drivers_rfm_table.quantile(q=[0.25,0.5,0.75])
quantiles = quantiles.to_dict()

quantiles_r = drivers_rfm_table.quantile(q=[0.33,0.66])
quantiles_r = quantiles_r.to_dict()


# In[11]:


segmented_rfm=drivers_rfm_table


# In[12]:


#fixed Recency figures
#Quartile Frequency &  

def RScore(x,p,d):
    if x <= d[p][0.25]:
        return 1
    elif x <= d[p][0.50]:
        return 2
    elif x <= d[p][0.75]: 
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


# In[13]:


segmented_rfm['r_quartile'] = segmented_rfm['recency'].apply(RScore, args=('recency',quantiles,))
segmented_rfm['f_quartile'] = segmented_rfm['frequency'].apply(FMScore, args=('frequency',quantiles,))
segmented_rfm['m_quartile'] = segmented_rfm['monetary_value'].apply(FMScore, args=('monetary_value',quantiles,))
segmented_rfm.head()


# In[14]:


segmented_rfm['RFMScore'] = segmented_rfm.r_quartile.map(str) + segmented_rfm.f_quartile.map(str) + segmented_rfm.m_quartile.map(str)
segmented_rfm.head()


# In[15]:


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


# In[16]:


segmented_rfm['segment_type']="active"
seg_test=segmented_rfm[['id_driver','RFM_segment','RFMScore','r_quartile','f_quartile','m_quartile','recency','frequency','monetary_value','segment_type','updated_at']]
# Seg_test is the final table, result of assigning a segment to each RFM Score.
seg_test.head()


# In[11]:


## chunk of code that inserts data to DB - not useful here
# def chunks_generator(my_list, chunk_size):
#     """Yield successive n-sized chunks from list my_list."""
#     for i in range(0, len(my_list), chunk_size):
#         yield my_list[i:i + chunk_size]

# seg_test
# def f(t):
#     return t.dt.strftime('%Y-%m-%d %H:%M:%S').map("TIMESTAMP '{}'".format, na_action=None)


# df_tmp = seg_test.select_dtypes(include=["datetime64[ns]"]).apply(f)
# for col in df_tmp.columns:
#     seg_test[col] = df_tmp[col]
# table = "bi_driver_segment"
# for data in chunks_generator([str(tuple(row)) for row in seg_test.values], 5000):
#     data = map(lambda el: el.replace('"', ''), data)
#     query = "INSERT INTO " + table + " VALUES " + ", ".join(data) 
#     pd.read_sql_query(query,cnxn)
# print(str(len(seg_test)) + " rows have been inserted to " + table)


# In[18]:


# Similar process for Active Passengers
# Active Passengers data import
passengers_rfm_table = pd.read_csv("bi_passenger_rfm_data.csv")
len(passengers_rfm_table)


# In[19]:


quantiles = passengers_rfm_table.quantile(q=[0.25,0.5,0.75])
quantiles = quantiles.to_dict()

quantiles_r = passengers_rfm_table.quantile(q=[0.33,0.66])
quantiles_r = quantiles_r.to_dict()
segmented_rfm=passengers_rfm_table


# In[20]:


def RScore(x,p,d):
    if x <= d[p][0.25]:
        return 1
    elif x <= d[p][0.50]:
        return 2
    elif x <= d[p][0.75]: 
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


# In[21]:


segmented_rfm['r_quartile'] = segmented_rfm['recency'].apply(RScore, args=('recency',quantiles,))
segmented_rfm['f_quartile'] = segmented_rfm['frequency'].apply(FMScore, args=('frequency',quantiles,))
segmented_rfm['m_quartile'] = segmented_rfm['monetary_value'].apply(FMScore, args=('monetary_value',quantiles,))
segmented_rfm.head()


# In[22]:


segmented_rfm['RFMScore'] = segmented_rfm.r_quartile.map(str) + segmented_rfm.f_quartile.map(str) + segmented_rfm.m_quartile.map(str)
segmented_rfm.head()


# In[23]:


segmented_rfm.groupby('f_quartile')['frequency'].describe()
segmented_rfm.groupby('m_quartile')['monetary_value'].describe()


# In[24]:


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


# In[25]:


segmented_rfm['segment_type']="active"
seg_test=segmented_rfm[['id_passenger','RFM_segment','RFMScore','r_quartile','f_quartile','m_quartile','recency','frequency','monetary_value','segment_type','updated_at']]
seg_test.head()


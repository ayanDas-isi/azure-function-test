# -*- coding: utf-8 -*-
"""
Created on Sun Sep 26 19:16:06 2021

@author: 0193A5744
"""

from azure.storage.blob import BlobClient
import pandas as pd
import datetime
def fn_storeDF(df):
    today=datetime.datetime.today()
    df.to_csv('/tmp/' +'mf_'+str(today.year)+'-'+str(today.month)+'-'+str(today.day)+'.csv')
    blob = BlobClient.from_connection_string(conn_str="DefaultEndpointsProtocol=https;AccountName=storageaccayan;AccountKey=fsU3Fi6rIjLERjfLoet87EV97VPh7VllnoRE3DgZYRcnLJAqbuJxnX2hIuJkMTGJfdgnljoSSlWTowHHRTQX2A==;EndpointSuffix=core.windows.net",
     container_name="mf-portfolio", blob_name='mf_'+str(today.year)+'-'+str(today.month)+'-'+str(today.day)+'.csv')
    
    with open('/tmp/' +'mf_'+str(today.year)+'-'+str(today.month)+'-'+str(today.day)+'.csv', "rb") as data:
        blob.upload_blob(data,overwrite=True)
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 28 17:02:12 2020

@author: AyanDas
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Jul 10 11:07:08 2020

@author: AyanDas
"""

# -*- coding: utf-8 -*-
"""
Created on Sat Jun 13 10:53:12 2020

@author: AyanDas
"""

import ibm_boto3
import json
import requests
from ibm_botocore.client import Config
import pandas as pd
#with open('cos_credentials_john.json') as data_file:
#    credentials = json.load(data_file)


obj_sto_land_test_cred={
  "apikey": "mxvEhwGq284KYxfwtbDWQhLMCYj6aD786TwIYjFZB3n-",
  "endpoints": "https://control.cloud-object-storage.cloud.ibm.com/v2/endpoints",
  "iam_apikey_description": "Auto-generated for key 24af6f0c-9913-40b2-8766-3522da5874bb",
  "iam_apikey_name": "UPS_Landing_Prod",
  "iam_role_crn": "crn:v1:bluemix:public:iam::::serviceRole:Writer",
  "iam_serviceid_crn": "crn:v1:bluemix:public:iam-identity::a/54ba5c628eb3b9741efc80bd432510aa::serviceid:ServiceId-9d8ac1e7-f6a8-4a89-9eec-c1012c318893",
  "resource_instance_id": "crn:v1:bluemix:public:cloud-object-storage:global:a/54ba5c628eb3b9741efc80bd432510aa:2750d2b3-fc07-48c5-97d3-c65571fa95d4::"
}


# Rquest detailed enpoint list
endpoints = requests.get(obj_sto_land_test_cred.get('endpoints')).json()
# Obtain iam and cos host from the the detailed endpoints
iam_host = (endpoints['identity-endpoints']['iam-token'])
cos_host = (endpoints['service-endpoints']['regional']['us-south']['public']['us-south'])
#cos_host2 = (endpoints['service-endpoints']['regional']['us-east']['public']['us-east'])
api_key = obj_sto_land_test_cred.get('apikey')
service_instance_id = obj_sto_land_test_cred.get('resource_instance_id')
# Constrict auth and cos endpoint
auth_endpoint = "https://" + iam_host + "/oidc/token"
service_endpoint = "https://" + cos_host


cos = ibm_boto3.client('s3',
                    ibm_api_key_id=api_key,
                    ibm_service_instance_id=service_instance_id,
                    ibm_auth_endpoint=auth_endpoint,
                    config=Config(signature_version='oauth'),
                    endpoint_url=service_endpoint)



#cos=cos2
def get_full_bucket_contents(bucket_name,prefix_name):
    '''
    retrieves all keys (file names) from an entire Object Storage bucket
    '''
    fnameList=[]
    nextresponse = cos.list_objects_v2(Bucket=bucket_name,Prefix=prefix_name)
    for fileN in nextresponse['Contents']:
        #print(fileN)
        fnameList.append((fileN['Key'],fileN['Size']/(1024*1024),fileN['LastModified']))#
    isTruncated = nextresponse['IsTruncated']
    while isTruncated:
        nextresponse = cos.list_objects_v2(Bucket=bucket_name, ContinuationToken=nextresponse['NextContinuationToken'],
                                           Prefix=prefix_name)
        isTruncated = nextresponse['IsTruncated']
        for fileN in nextresponse['Contents']:
            fnameList.append((fileN['Key'],fileN['Size']/(1024*1024),fileN['LastModified']))#
    return fnameList


def download(ipfile,opfile,bucket):
        
    print('Downloading starts')
    
    cos.download_file(Bucket= bucket,Key = ipfile,Filename=opfile)
    
    print('Downloading completes!')
    
def upload(ipfileName,opfile,bucket):
    print('uploading',opfile)
    cos.upload_file(Filename=ipfileName,Bucket=bucket,Key=opfile)
    print('uploading done')

#a=get_full_bucket_contents('ups-invoices-logging-bucket-prod-it','2021-09-01')# 


def download_obj(bucket,fname):
    s=cos.get_object(Bucket= bucket,Key = fname)
    return s['Body'].read().decode('utf-8') 

def getJSONObject(bucket,fname):
    a=download_obj(bucket,fname)
    return json.loads(a)
        
#b=getJSONObject('ups-invoices-logging-bucket-prod-it','2021-09-01-e21d0c75-0ab7-11ec-b888-c77054b7ad06_214842_01092021000223173.json')

def getDFObject(bucket,fname):
    body=download_obj(bucket,fname)
    f=pd.compat.StringIO(body)
    empId={'Orgnl Employee UPS Id Num':str,'Pkg Svc Provider UPS Id Num':str,
           'Employee UPS Id Num':str,'Center Num':str,'GTS TC Pay Cd':str,'Street Num':str,
           'Suite Num':str,'SLIC':str,'EmployeeID':str}
    #'Orgnl Employee UPS Id Num','Employee UPS Id Num','Pkg Svc Provider UPS Id Num'
    return pd.read_csv(f,error_bad_lines=False,dtype=empId)

#aa=get_full_bucket_contents('driver-platform-phase1','Daily Reports/SSP_GeofenceAM-PM_')

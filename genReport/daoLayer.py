# -*- coding: utf-8 -*-
"""
Created on Thu Sep 16 11:19:55 2021

@author: 0193A5744
"""
import pymongo
import time
import datetime
from bson.objectid import ObjectId
from pymongo import UpdateOne
import pandas as pd

myclient = pymongo.MongoClient('mongodb://cosmos-acc-ayan:j3VhWD3MQEJ9f2l0hcdEvNBdPKtzYioCDRXFm3oHIyTa6B9ytSQ0JmQ3GT0wf2u1gPPuNRN46jUqeeD2hjuLNQ%3D%3D@cosmos-acc-ayan.mongo.cosmos.azure.com:10255/?authSource=admin&replicaSet=globaldb&maxIdleTimeMS=120000&readPreference=primary&appname=MongoDB%20Compass%20Community&retryWrites=false&ssl=true',
                               ssl=True)
mydb = myclient['MFSchema']#

#print(DBRef(collection = "subjects", id = '5ef09c1fcb06406753198e13'))
#c=mydb['students'].create_index([("studentName", pymongo.ASCENDING),
#                                    ("className", pymongo.DESCENDING)] ,unique=True)
# =============================================================================
# a=mydb['students.subjects'].find({'subject.$sub1':'CS'})
# for b in a :
#     print(b)
# =============================================================================

def pull_data(aggList,tab):
    a=mydb[tab].aggregate(aggList)
    return list(a)


def update_log(objid,key,value,uptype='set'):
    updateDic={key: value}
    if key=='error':
        updateDic['status']='ERROR'
    if key=='status':
        a=mydb['dailyDataProcessing'].update_one({'_id':ObjectId(objid)}, 
              {'$push':{'log':{'status_to':value,'time':datetime.datetime.today()}} }, False, None, None)
    a=mydb['dailyDataProcessing'].update_one({'_id':ObjectId(objid)}, 
      {'$'+uptype:updateDic }, False, None, None)
    return a



def get_prop(key):
    result = mydb['propertiesTable'].aggregate(
            [
            {
                '$project': {
                    key: 1
                }
            }
        ])
    for res in result:
        return res[key]
    

def create_update_list(find,update,updatelist,up_sert):
    #print(UpdateOne(find,update,upsert=up_sert))
    updatelist.append(UpdateOne(find,update,upsert=up_sert))
    return updatelist

def bulk_update(updatelist,tablename):
    if len(updatelist)==0:
        return '0 values to update'
    result = mydb[tablename].bulk_write(updatelist)
    #print(result)
    return result

def insert_holdings():
    df=pd.read_csv('allfunds.csv')
    hold=[]
    for i,row in df.iterrows():
        hold=create_update_list({'code':row['code'],'purchaseDate':datetime.datetime.strptime(row['purchaseDate'], '%d-%m-%Y')},
                                 {'$set':{'fundName':row['fundName'],'units':row['units'],'gainList':[],'invType':row['type']}},hold,up_sert=True)
    a=bulk_update(hold,'holdings')
    print(a.bulk_api_result)
    
#insert_holdings()
    
def insert_gain():
    df=pd.read_csv('allfunds.csv')
    today=datetime.datetime.today()
    hold=[]
    for i,row in df.iterrows():
        hold=create_update_list({'code':row['code'],'purchaseDate':datetime.datetime.strptime(row['purchaseDate'], '%d-%m-%Y')},
                                 {'$push':{'gainList':{'gain':row['monthly_gain'],'date':today}}},hold,up_sert=True)
    bulk_update(hold,'holdings')
    

'''
df=pd.read_csv('allfunds_monthly.csv')

hold=[]
for i,row in df.iterrows():
    for i in range(2,10):
        evalDate=datetime.datetime(2021,i,1)-datetime.timedelta(days=1)
        
        hold=create_update_list({'code':row['code'],'purchaseDate':datetime.datetime.strptime(row['purchaseDate'], '%d-%m-%Y')},
                             {'$push':{'gainList':{'gain':row['monthly_gain_'+str(evalDate.date())],'date':evalDate}}},hold,up_sert=True)
bulk_update(hold,'holdings')
'''
#if 1st date of month then store yearly and monthly gain
#if 1st date of month store current mf asset
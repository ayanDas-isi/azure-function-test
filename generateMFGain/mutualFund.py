# -*- coding: utf-8 -*-
"""
Created on Mon Sep 13 17:55:55 2021

@author: 0193A5744
"""
import pandas as pd
from mftool import Mftool
import datetime
import json
from .daoLayer import pull_data
import os
import glob
from . import storeDF

def deleteall():
    files = glob.glob('./nav/')
    for f in files:
        os.remove(f)
    
    
obj = Mftool()

data = obj.get_scheme_codes() 
#print(data)

def get_fund(fname,fundType='Direct Plan',scheme='Growth'):
    fname=fname.lower()
    for k in data.keys():
        #print(v,k)
        if fname in str(data[k]).lower():
            print(k,data[k])
            if fundType.lower() in str(data[k]).lower():
                if scheme.lower() in str(data[k]).lower():
                    print('-------------------',k,data[k])
            

def get_nav_val(cd):
    data = obj.get_scheme_historical_nav(code=cd)
    print(data['data'][0])
    return data

def get_start_nav(today,data,startDate):  
    nav=0
    if today<startDate:
        return 0,0
    endNAV=None
    #print(monthStart)
    for dtval in data['data']:
        thisDate = datetime.datetime.strptime(dtval['date'], '%d-%m-%Y')
        #print(thisDate,monthStart)
        if thisDate<today and endNAV==None:
            endNAV=float(dtval['nav'])
        if thisDate<startDate:
            #print('returning ',dtval)
            return float(nav),endNAV
        nav=dtval['nav']
    return float(nav),endNAV

def get_date_unit(today,code,amount):
    path='./nav/data_'+str(code)+'_'+str(datetime.datetime.today().date())+'.json'
    if os.path.exists(path)==True:
        f = open(path)
        data = json.load(f)
    else:
        data = obj.get_scheme_historical_nav(code=code)
        with open(path, 'w') as f:
            json.dump(data, f)
    nav=0
    #print(monthStart)
    for dtval in data['data']:
        thisDate = datetime.datetime.strptime(dtval['date'], '%d-%m-%Y')
        print(thisDate,today)
        
        if thisDate<today:
            #print('returning ',dtval)
            return amount/float(nav)
        nav=dtval['nav']
    return amount/float(nav)


#holdings=pd.read_csv('allfunds.csv')


def addSIP(calcDate,holdings):
    sipList=[] 
    for i,row in holdings[holdings['invType']=='S'].iterrows():
        startDate =row['purchaseDate']# datetime.datetime.strptime(row['purchaseDate'], '%d-%m-%Y')
        while True:
            addDate=startDate+datetime.timedelta(days=15)
            if addDate>=calcDate:
                break
            sipList.append({'code':row['code'],'fundName':row['fundName'],'purchaseDate':addDate.strftime('%d-%m-%Y'),
                            'type':'A','units':get_date_unit(addDate,row['code'],5000)})
            startDate=addDate
            print(startDate,row['code'])
    sipList=pd.DataFrame(sipList)
    holdings=pd.concat([holdings,sipList])
    holdings.to_csv('holdings_sip.csv')
    return holdings
    


#%%
def get_recent_increase(data):
    if len(data)>201:
        recentInc=(float(data[0]['nav']) - float(data[5]['nav']))/float(data[5]['nav'])
        pastInc=(float(data[6]['nav']) - float(data[200]['nav']))/(39*float(data[200]['nav']))
        return recentInc,pastInc
    else:
        return 0.0,0.0
    
def last_day_gain(row,unit):
    #print(row)
    data=row['data']
    return (float(data[0]['nav']) - float(data[1]['nav']))*unit

def get_gain(row):
    today=datetime.datetime.today()
    monthStart=datetime.datetime(today.year,today.month,1)
    yearStart=datetime.datetime(today.year,1,1)
    path='./nav/data_'+str(row['code'])+'_'+str(today.date())+'.json'
    if type(row['purchaseDate'])==str:
        purDate =datetime.datetime.strptime(row['purchaseDate'], '%d-%m-%Y')
    else:
        purDate = row['purchaseDate']#datetime.datetime.strptime(row['purchaseDate'], '%d-%m-%Y')
    if os.path.exists(path)==True:
        f = open(path)
        data = json.load(f)
    else:
        data = obj.get_scheme_historical_nav(code=row['code'])
        with open(path, 'w') as f:
            json.dump(data, f)
    recentInc,pastInc=get_recent_increase(data['data'])
    lastDGain=last_day_gain(data,row['units'])
    print(row['fundName'])
    if today.month>purDate.month or today.year >purDate.year:
        startNAV,curNAV=get_start_nav(today,data,monthStart)
       
    else:
        startNAV,curNAV=get_start_nav(today,data,purDate)
        

    if today.year >purDate.year:
        yrStartNAV,x=get_start_nav(today,data,yearStart)
       
    else:
        yrStartNAV,x=get_start_nav(today,data,purDate)
    #print()
    return (curNAV-startNAV)*row['units'],(curNAV-yrStartNAV)*row['units'], 100*(curNAV-startNAV)/startNAV,100*(curNAV-yrStartNAV)/yrStartNAV,recentInc,pastInc,lastDGain

def get_monthly_gain(row,evalDate):
    thisMonthStart=datetime.datetime(evalDate.year,evalDate.month,1)
    path='./nav/data_'+str(row['code'])+'_'+str(today.date())+'.json'
    purDate = datetime.datetime.strptime(row['purchaseDate'], '%d-%m-%Y')
    if os.path.exists(path)==True:
        f = open(path)
        data = json.load(f)
    else:
        data = obj.get_scheme_historical_nav(code=row['code'])
        with open(path, 'w') as f:
            json.dump(data, f)
    #print(row['fundName'])
    #if evalDate.month>purDate.month or evalDate.year >purDate.year:
    if thisMonthStart>purDate:
        startNAV,curNAV=get_start_nav(evalDate,data,thisMonthStart)
       
    else:
        startNAV,curNAV=get_start_nav(evalDate,data,purDate)

    #print()
    return (curNAV-startNAV)*row['units']
        

'''
for i in range(2,10):
    evalDate=datetime.datetime(2021,i,1)-datetime.timedelta(days=1)
    holdings['monthly_gain_'+str(evalDate.date())]=holdings.apply(get_monthly_gain,evalDate=evalDate,axis=1)
    print(f'total monthly gain {round(sum( holdings["monthly_gain_"+str(evalDate.date())])/1000.0,2)}k')

holdings.to_csv('allfunds_monthly.csv')
'''
def evaluate():
    a=[{'$project':{'gainList':0,'_id':0}}]
    holdings=pd.DataFrame(pull_data(a,'holdings'))
    
    
    today=datetime.datetime.today()
    holdings=addSIP(today,holdings)
    holdings[['monthly_gain','yearly_gain','incPercent','yrIncPercent','recentInc','pastInc','lastDGain']]=holdings.apply(get_gain,axis=1,result_type="expand")
    print(f'total monthly gain {round(sum(holdings["monthly_gain"])/1000.0,2)}k ,yearly gain {round(sum(holdings["yearly_gain"])/1000.0,2)}k')
    holdings.to_csv('allfunds_gain.csv')
    #deleteall()
    storeDF.storeDF(holdings)
    

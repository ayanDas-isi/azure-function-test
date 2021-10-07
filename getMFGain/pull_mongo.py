# -*- coding: utf-8 -*-
"""
Created on Thu Oct  7 11:59:51 2021

@author: 0193A5744
"""
import pymongo
myclient = pymongo.MongoClient('mongodb://cosmos-acc-ayan:j3VhWD3MQEJ9f2l0hcdEvNBdPKtzYioCDRXFm3oHIyTa6B9ytSQ0JmQ3GT0wf2u1gPPuNRN46jUqeeD2hjuLNQ%3D%3D@cosmos-acc-ayan.mongo.cosmos.azure.com:10255/?authSource=admin&replicaSet=globaldb&maxIdleTimeMS=120000&readPreference=primary&appname=MongoDB%20Compass%20Community&retryWrites=false&ssl=true',
                               ssl=True)
mydb = myclient['MFSchema']#


def pull_data(aggList,tab):
    a=mydb[tab].aggregate(aggList)
    return list(a)


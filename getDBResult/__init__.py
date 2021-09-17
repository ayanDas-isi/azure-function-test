import logging

import azure.functions as func

import pymongo
import json
import datetime
from bson.objectid import ObjectId


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

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    
    req_body = req.get_json()
    tab = req_body.get('tabName')
    agg=req_body.get('agg')
    resp=pull_data(agg,tab)
    return func.HttpResponse(
        json.dumps(resp, default=str),
        mimetype="application/json",
    )

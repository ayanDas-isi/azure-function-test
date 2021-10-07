# -*- coding: utf-8 -*-
"""
Created on Wed Oct  6 12:55:27 2021

@author: 0193A5744
"""
from math import pi

from bokeh.io import output_file, show
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, CustomJS, Slider
from bokeh.plotting import figure
from datetime import date
import calendar
import pymongo
import datetime

from bokeh.models.widgets import DateSlider
from bokeh.transform import cumsum
import pandas as pd

myclient = pymongo.MongoClient('mongodb://cosmos-acc-ayan:j3VhWD3MQEJ9f2l0hcdEvNBdPKtzYioCDRXFm3oHIyTa6B9ytSQ0JmQ3GT0wf2u1gPPuNRN46jUqeeD2hjuLNQ%3D%3D@cosmos-acc-ayan.mongo.cosmos.azure.com:10255/?authSource=admin&replicaSet=globaldb&maxIdleTimeMS=120000&readPreference=primary&appname=MongoDB%20Compass%20Community&retryWrites=false&ssl=true',
                               ssl=True)
mydb = myclient['MFSchema']#


def pull_data(aggList,tab):
    a=mydb[tab].aggregate(aggList)
    return list(a)


mfdata= pull_data([{'$project':{'_id':0}}],'monthlyMFGain')
color_code=['#68FF33','#33DCFF','#33B3FF','#5033FF','#C8FF33','#DB33FF','#1F84AF']

def get_distribution(gainList):            
    allGain={x['fundCode']:x['monthlyGain'] for x in gainList if x['monthlyGain']>0}
    totValue=sum(allGain.values())
    startAngle=0
    startAngleList=[]
    codeList=[]
    colorList=[]
    
    
    for i,k in enumerate(allGain):
        #startAngleList.append(startAngle/Divider)
        startAngle=startAngle+allGain[k]
        #endAngleList.append(startAngle/Divider)
        startAngleList.append(allGain[k]/totValue * 2*pi)
        codeList.append(k)
        colorList.append(color_code[i%len(color_code)])
    
    
    return startAngleList,colorList,codeList
    
startAngleList,colorList,codeList=get_distribution(mfdata[5]['fundwise'])

source = ColumnDataSource(data=dict(
        start=startAngleList, color=colorList,code=codeList
    ))

allDates={x['endDate'].date():x['fundwise'] for x in mfdata}
#%%

output_file("pie.html")



plot = figure()
plot.wedge(x=0, y=0, start_angle=cumsum('start', include_zero=True), end_angle=cumsum('start'), radius=1,
        color='color', alpha=0.6, source=source,legend='code')

#slider = Slider(start=.1, end=1., value=.2, step=.1, title="delta-V")
date_range_slider  = DateSlider(title="Date Range: ", start=min(allDates.keys()), end=max(allDates.keys()), value=max(allDates.keys()), step=1)


def update(source=source, slider=date_range_slider, window=None):
    #mr=calendar.monthrange(slider.value.year, slider.value.month)
    data = source.data
    data['end'][0] = slider.value
    source.change.emit()

date_range_slider.js_on_change('value', CustomJS.from_py_func(update))

show(column(date_range_slider, plot))
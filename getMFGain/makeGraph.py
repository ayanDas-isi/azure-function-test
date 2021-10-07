# -*- coding: utf-8 -*-
"""
Created on Sun Sep 26 12:38:28 2021

@author: 0193A5744
"""
import numpy as np
import pandas as pd
from bokeh.models import HoverTool
from bokeh.models import ColumnDataSource
from bokeh.layouts import gridplot
from bokeh.document import Document
from bokeh.layouts import column,row
from bokeh.models import Div
from bokeh.io import curdoc
from bokeh.embed import file_html
from bokeh.resources import INLINE
from bokeh.models import  FactorRange

from bokeh.io import show
from bokeh.models import Button, CustomJS
from azure.storage.blob import BlobClient
import datetime
from io import StringIO

try:
    from . import pull_mongo
except:
    import pull_mongo

#hold=pd.read_csv('allfunds_gain.csv')


from bokeh.io import show, output_file
from bokeh.plotting import figure
from bokeh.models import Range1d
from bokeh.palettes import Spectral6
#output_file("bar_basic.html")

color_code=['#68FF33','#33DCFF','#33B3FF','#5033FF','#C8FF33','#DB33FF','#1F84AF']
color_code.extend(Spectral6)
def plot_increase(agg,yVal,title):
    #hold=hold[hold['monthly_gain']>0]
    

    hold=agg.sort_values(by=yVal,ascending=False)
    hold['fundName']=hold['fundName'].apply(lambda x: x.split('-')[0])
    hold['comp']=hold['fundName'].apply(lambda x: x.split(' ')[0])
    colorDic={com:color_code[i] for i,com in enumerate(set(hold['comp']))}
    hold['color']=hold['comp'].apply(lambda x: colorDic[x])
    src = ColumnDataSource(hold)
    
    valList=list(agg[yVal])
    minVal,maxVal=min(valList),max(valList)
    p = figure(x_range=list(hold['fundName']),  plot_height=450, title=title, 
               sizing_mode="stretch_width",
               toolbar_location=None, tools="")
    
    p.vbar(x='fundName',source=src, top=yVal, width=0.9, color='color',legend="comp")
    
    p.xgrid.grid_line_color = None
    p.y_range.start = 0
    hover = HoverTool(
        tooltips=[
         ("fund", '@fundName'),
            ("gain", "@"+yVal)
            ]#,formatters={'@date': 'datetime'}
        ) 
    hover.mode = 'mouse'
    p.add_tools(hover)
    p.y_range = Range1d(minVal-1,maxVal+1)
    #show(p)
    return p

def form_lineGraph(gainList,dateList):
    fig = figure(title='Monthly Gain',
             x_axis_label='x',
             y_axis_label='y',
             sizing_mode="stretch_width",
             height=450,x_axis_type='datetime')
    fig.line(dateList,gainList, legend='Monthly Gain', color='blue', line_width=2)
    fig.circle(dateList,gainList, legend='Monthly Gain', color='blue', fill_color='white', size=5)
    return fig

def categorical_plot(holding,colorMap):
    codeCate=dict(zip(holding.fundName, holding.category))
    holding['perInc']=holding['recentInc']-holding['pastInc']
    holding=holding.sort_values(by='perInc',ascending=False)
    fruits =list(holding['fundName'])# ['Apples', 'Pears', 'Nectarines', 'Plums', 'Grapes', 'Strawberries']
    years = ['recentInc', 'pastInc']
    
    data = {'fundName' : fruits,
            'recentInc'  : list(holding['recentInc']),
            'pastInc' :list(holding['pastInc'])}
    
    # this creates [ ("Apples", "2015"), ("Apples", "2016"), ("Apples", "2017"), ("Pears", "2015), ... ]
    x = [ (fruit, year) for fruit in fruits for year in years ]
    color=[colorMap[codeCate[fundName]] for (fundName,y) in x]
    counts = sum(zip(data['recentInc'], data['pastInc']), ()) # like an hstack
    #colorDic={com:color_code[i] for i,com in enumerate(set(hold['comp']))}
    legends=[codeCate[fundName].replace('Fund','') for (fundName,y) in x]
    source = ColumnDataSource(data=dict(x=x, counts=counts,colors=color,legend=legends))
    
    p = figure(x_range=FactorRange(*x), height=300, title="increase difference",
               toolbar_location=None, tools="",sizing_mode="stretch_width")
    
    p.vbar(x='x', top='counts', width=0.9, source=source,fill_color='colors',legend="legend")
    
    p.y_range.start = 0
    p.x_range.range_padding = 0.1
    p.xaxis.major_label_orientation = 1
    p.xgrid.grid_line_color = None
    p.legend.location = "top_right"
    p.legend.orientation = "horizontal"

    hover = HoverTool(
        tooltips=[
         ("fund", '@x'),
            ("counts", "@counts")
            ]#,formatters={'@date': 'datetime'}
        ) 
    hover.mode = 'mouse'
    p.add_tools(hover)
    p.y_range = Range1d(min([min(data['recentInc']),min(data['pastInc'])])-0.01,max([max(data['recentInc']),max(data['pastInc'])])+0.02)
    return p

def add_level(text):
    button = Button(label=text, button_type="success")
    button.js_on_click(CustomJS(code="console.log('button: click!', this.toString())"))
    return button
    #show(button)

def generate_html():
    today=datetime.datetime.today()
    for i in range(3):
        try:
            blob = BlobClient.from_connection_string(conn_str="DefaultEndpointsProtocol=https;AccountName=storageaccayan;AccountKey=fsU3Fi6rIjLERjfLoet87EV97VPh7VllnoRE3DgZYRcnLJAqbuJxnX2hIuJkMTGJfdgnljoSSlWTowHHRTQX2A==;EndpointSuffix=core.windows.net",
                container_name="mf-portfolio", blob_name='mf_'+str(today.year)+'-'+str(today.month)+'-'+str(today.day)+'.csv')
            stream = blob.download_blob()
            data =stream.readall()
            s=str(data,'utf-8')
            
            data = StringIO(s) 
            
            hold=pd.read_csv(data)
            break
        except:
            today=today-datetime.timedelta(days=i+1)
            continue
            
        
    agg=hold.groupby(by='fundName').aggregate({'monthly_gain':'sum','incPercent':'first','recentInc':'first',
                    'pastInc':'first','category':'first'}).reset_index()
    #return agg
    colorMap={cate:color_code[i%len(color_code)] for i,cate in enumerate(list(agg['category']))}
    p1=plot_increase(agg,'monthly_gain',"Monthly increase")
    p2=plot_increase(agg,'incPercent',"Monthly increase %")
    p3=categorical_plot(agg,colorMap)
    
    a=pull_mongo.pull_data([{'$project':{'_id':0,'fundwise':0}},{'$sort':{'endDate':1}}],'monthlyMFGain')
    p4=form_lineGraph([data['gain'] for data in a],[data['endDate'] for data in a])
    
    l0=add_level('Report date:'+str(today.year)+'-'+str(today.month)+'-'+str(today.day))
    l1=add_level(f'this year gain:{int(sum(hold["yearly_gain"])/1000)}k')
    l2=add_level(f'this month gain:{int(sum(hold["monthly_gain"])/1000)}k')
    l3=add_level(f'today\'s gain:{int(sum(hold["lastDGain"]))}')
    rowLevel=row(l0,l1,l2,l3,sizing_mode="scale_width")
    
    plotlist= gridplot([[rowLevel],[p1],[p2],[p4],[p3]], toolbar_location=None)
    
    div = Div(text="""
        <h1>Performance analysis</h1>
        
        """)
    
    doc = Document()
    doc.add_root(column(div, plotlist, sizing_mode="scale_width"))
    
    curdoc().theme = 'dark_minimal'
    doc.validate()
    
    filename = "/tmp/MF_analysisV1.html"#
    with open(filename, "w", encoding="utf-8") as f:
        f.write(file_html(doc, INLINE,'MF_analysis'))
    print("Wrote %s" % filename)
    return filename
from bokeh.io import show, output_notebook
from bokeh.models import ColumnDataSource
from bokeh.palettes import Spectral6
from bokeh.plotting import figure
from bokeh.transform import factor_cmap
from bokeh.palettes import Category20
from bokeh.io import output_notebook
output_notebook()
import pandas as  pd
import numpy as np
from bokeh.models import HoverTool
from bokeh.transform import cumsum
from bokeh.models import FactorRange
from bokeh.transform import factor_cmap
from  datetime import datetime
from cassandra.cluster import Cluster
cluster = Cluster(["127.0.0.1"], port="9042")
session =   cluster.connect('creditcard')

def chart4():
    ## get data
    stmt = "SELECT * FROM  view_fraud_transaction_by_category_year ;"
    data = session.execute(stmt)
    df_yes = pd.DataFrame([ d for d in data])
     
    ## Transaction per Category 
    
    fraud_cate_count = df_yes.sort_values(by='category')
  
    years = ['2019', '2020']
    categorys = np.array(fraud_cate_count['category'].unique()).reshape(-1).tolist()
    color =["#c9d9d3", "#718dbf", "#e84d60" , "#033649" ]

    hover_tool = HoverTool(tooltips=[
        ("Category:Year", "@x")
    ])

    yes_fraud = {
        'category': categorys,
        '2019' : np.array(fraud_cate_count.loc[fraud_cate_count['year'] ==2019, ['count']].values).reshape(-1).tolist(),
        '2020' : np.array(fraud_cate_count.loc[fraud_cate_count['year'] ==2020, ['count']].values).reshape(-1).tolist()}


    x = [ (category, year) for category in categorys for year in years ]
    counts = sum(zip(yes_fraud['2019'], yes_fraud['2020'] ), ()) 

    source = ColumnDataSource(data=dict(x=x, counts=counts))

    p = figure(x_range=FactorRange(*x), height=450, width=1200 , toolbar_location=None, tools=[hover_tool],
             title="Fraud Counts per Category by Year")


    p.vbar(x='x', top='counts', width=0.9, source=source, line_color="white",
       fill_color=factor_cmap('x', palette=['firebrick', 'olive'], factors=years, start=1, end=2))

    p.y_range.start = 0
    p.x_range.range_padding = 0.1
    p.xaxis.major_label_orientation = 1
    p.xgrid.grid_line_color = None
    p.title.align = "center"
    return p



   


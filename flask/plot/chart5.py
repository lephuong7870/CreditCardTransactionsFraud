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

def chart5():
    
    ## get data
    stmt = "SELECT * FROM  view_fraud_transaction_by_month ;"
    data = session.execute(stmt)
    df_yes = pd.DataFrame([ d for d in data])

    ## Transaction per Category 
    df_yes =df_yes.sort_values(by='month')
    factors = [("Q1", "jan"), ("Q1", "feb"), ("Q1", "mar"),
           ("Q2", "apr"), ("Q2", "may"), ("Q2", "jun"),
           ("Q3", "jul"), ("Q3", "aug"), ("Q3", "sep"),
           ("Q4", "oct"), ("Q4", "nov"), ("Q4", "dec")]

    data = list(df_yes['count'])
    p = figure(x_range=FactorRange(*factors), height=450, width=1200 , title="Fraud Counts per Month")
    p.vbar(x=factors, top=data, width=0.9, alpha=0.5)

    p.line(x=factors, y=data, color="red", line_width=3)
    p.circle(x=factors, y=data, line_color="red",  fill_color="white", size=10)

    p.y_range.start = 0
    p.x_range.range_padding = 0.1
    p.xgrid.grid_line_color = None
    p.title.align = "center"

    return p



   


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
from cassandra.cluster import Cluster
cluster = Cluster(["127.0.0.1"], port="9042")
session =   cluster.connect('creditcard')

def chart3():
    
    ## get data
    stmt = "SELECT * FROM  view_transaction_by_category ;"
    data = session.execute(stmt)
    df_all = pd.DataFrame([ d for d in data])

    ## Transaction per Category 
    
    df_cate_count = df_all.sort_values('count', ascending=False)

    source = ColumnDataSource(df_cate_count)


    hover_tool = HoverTool(tooltips=[
        ("category", "@category"),
        ("count", "@count")
    ])
 
    Count_category = figure(x_range= df_cate_count['category'].values ,width=1200 ,height=450, 
                        title="Count Transaction by category", toolbar_location=None, tools=[hover_tool])

    Count_category.vbar(x='category', top='count', width=0.4, source=source, color='#99DBF5' )

    Count_category.xgrid.grid_line_color = None
    Count_category.y_range.start = 0
    Count_category.y_range.end = df_cate_count['count'].max() + 5 # Add some padding to the y range

    Count_category.xaxis.axis_label = "Category"
    Count_category.yaxis.axis_label = "Count Transaction"
    Count_category.title.align = "center"


    
    return Count_category
   

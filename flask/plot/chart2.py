from bokeh.io import  output_notebook
from bokeh.plotting import figure
from bokeh.palettes import Category20
from bokeh.io import output_notebook
from bokeh.models import HoverTool
output_notebook()
import pandas as  pd
import numpy as np
from cassandra.cluster import Cluster
cluster = Cluster(["127.0.0.1"], port="9042")
session =   cluster.connect('creditcard')

from bokeh.transform import cumsum

def chart2():
    ## get data
    stmt = "SELECT * FROM  view_gender_count_customer ;"
    data = session.execute(stmt)
    df_customer = pd.DataFrame([ d for d in data])

    ## Total Customer by Gender 
    
    pie = df_customer['gender'].value_counts()
    data = pd.DataFrame({'gender': pie.index, 'value': pie.values})

    data['angle'] = data['value']/data['value'].sum() * 2*np.pi
    data['color'] =  ["#e84d60" , "#033649"]

    hover_tool = HoverTool(tooltips=[
           ("gender", "@gender"),
           ("number customer", "@value")
    ])


    p = figure(width=500, height=300, title="Number Customer by Gender", toolbar_location=None, 
                   tools=[hover_tool] , x_range=(-0.5, 1.0))

    p.wedge(x=0, y=1, radius=0.35,
        start_angle=cumsum('angle', include_zero=True), end_angle=cumsum('angle'),
        line_color="white", fill_color='color', legend_field='gender', source=data)


    p.axis.axis_label=None
    p.axis.visible=False
    p.grid.grid_line_color = None
    p.title.align = "center"
    return p




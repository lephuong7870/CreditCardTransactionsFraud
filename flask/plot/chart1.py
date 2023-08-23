from bokeh.io import  output_notebook
from bokeh.models import ColumnDataSource
from bokeh.plotting import figure
from bokeh.io import output_notebook
output_notebook()
import pandas as  pd
from bokeh.models import HoverTool
from datetime import datetime
from cassandra.cluster import Cluster
cluster = Cluster(["127.0.0.1"], port="9042")
session =   cluster.connect('creditcard')

def chart1():
    
    ## get Data View
    
    stmt = "SELECT * FROM view_age_count_customer ;"
    data = session.execute(stmt)
    df_customer = pd.DataFrame([ d for d in data])


    ## Create Chart
    
    min_age = df_customer['age'].min() - 1
    max_age = df_customer['age'].max() + 1

    AGES = ['{0} - 40'.format(min_age), '40 - 65', '65 - {0}'.format(max_age)]


    age_groups = pd.cut(df_customer['age'], bins= [ min_age  , 40, 65, max_age ])
    age_number_customer = list(df_customer.groupby(age_groups)['count'].sum().values)



    data = pd.DataFrame({
        'age': AGES , 
        'num_customer': age_number_customer,
    })

    source = ColumnDataSource(data)
    hover_tool = HoverTool(tooltips=[
        ("age", "@age"),
        ("number", "@num_customer")
    ])

    p = figure(width=500, height=300, y_range=AGES, x_range=(200, 700), 
           title="Number Customer by Age of Customer, 2019—2020", toolbar_location=None, tools=[hover_tool] )
    p.hbar(y='age', right='num_customer', height=0.5, source=source)

    p.ygrid.grid_line_color = None
    p.outline_line_color = None

    return p





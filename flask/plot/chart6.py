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
from bokeh.palettes import Category20

def chart6(df_top10):
    
   
    ## Distribute per Category 
        pie = df_top10['category'].value_counts()
        data = pd.DataFrame({'category': pie.index, 'value': pie.values})
        data['angle'] = data['value']/data['value'].sum() * 2*np.pi
        data['color'] = Category20[len(pie)]

        val_counts= figure( title="Category Distribution", toolbar_location=None,plot_height=400,plot_width=400,
                            tools="hover", tooltips="@category : @value", x_range=(-0.5, 1.0))


        val_counts.wedge(x=0, y=1, radius=0.35,
        start_angle=cumsum('angle', include_zero=True), end_angle=cumsum('angle'),
        line_color="white", fill_color='color', legend_field='category', source=data)


        val_counts.axis.axis_label=None
        val_counts.axis.visible=False
        val_counts.grid.grid_line_color = None


        val_counts.title.align = "center"
        

        return val_counts

    


   


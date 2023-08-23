from bokeh.plotting import  figure

def chart7(df_top10):
    
   
    ## AMT 


    x = list(range(1,len(df_top10)+1))
    y = list(df_top10['amt'].values)



    p = figure(  title="AMT Transaction", plot_height=400,plot_width=400, x_range=(0,11))
    line_color = "red"
    line_dash = "dotted"
    line_dash_offset = 1
    legend_label = "Line AMT"    
    p.line(x, y, line_color = line_color,
           line_dash = line_dash,
           line_dash_offset = line_dash_offset,
           legend_label = legend_label)

    p.yaxis.axis_label = "AMT"

    return p
    


   


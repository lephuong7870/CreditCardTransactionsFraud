import pandas as pd

def datatable(df_top10):
    # create dataframe
    df_top10['Top10'] = list(range(1,len(df_top10)+1))
    df_top10 = df_top10[['Top10' ,'category' , 'merchant' , 'trans_time', 'amt' ]]
    # render dataframe as html
    datatable = df_top10.to_html(index=False)
    return datatable
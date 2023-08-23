import pandas as pd
from  datetime import datetime
import joblib
import numpy as np


def get_feature( df_event: pd.DataFrame , df_customer: pd.DataFrame):
    df = df_event.merge( df_customer , how='left' , left_on='cc_num' , right_on='cc_num') 
    from  datetime import datetime
    now = datetime.now()
    df['age'] = now.year - df['dob'].dt.year
   
    return df



def predict_model( df : pd.DataFrame):

    model = joblib.load('D:/demolambda/flask/model.pkl')
    feature = ['amt' , 'age' , 'distance' , 'max_amt',	'mean_amt'	,'min_amt']
    pre = df[feature]

    ## Model 
    prediction = model.predict(pre)
    predict = pd.DataFrame(prediction, columns= ['predict'] )
    result = pd.DataFrame(np.hstack((predict, df )) , columns= predict.columns.append(df.columns))
    return result




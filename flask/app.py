from flask import Flask
from flask import render_template, request
import pickle
import numpy as np
from cassandra.cluster import Cluster
import pandas as pd
import time
import json
import flask
import pandas as pd
from bokeh.embed import components
from bokeh.plotting import figure
from bokeh.resources import INLINE
from  model_build.utils import get_feature, predict_model
from flask_sqlalchemy import SQLAlchemy
from form import signUpForm,loginForm, loginForm_manager
from flask import Flask, redirect, url_for, render_template, request, session
app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True
app.secret_key =  'dfewfew123213rwdsgert34tgfd1234trgf'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///bank.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
cluster = Cluster(["127.0.0.1"], port="9042")
session_cassandra =   cluster.connect('creditcard')

db = SQLAlchemy(app)
app.app_context().push()


"""Model for User Manager"""
class Manager(db.Model):
    id = db.Column(db.Integer , primary_key=True)
    name = db.Column(db.String, unique = True)
    password = db.Column(db.String)

"""Model for User Customer"""
class Customer(db.Model):
    customer_id = db.Column(db.String, primary_key=True)
    password = db.Column(db.String)

db.create_all()
""""Create DB Manager """
manager1 = Manager(name='quanly1', password='123456')
manager2 = Manager(name='quanly2' , password='123456')
db.session.add(manager1)
db.session.add(manager2)
try:
    db.session.commit()
except Exception as e:
    db.session.rollback()
finally:
    db.session.close()


@app.route('/login' ,methods = ["POST","GET"])  
def login():
    form = loginForm_manager()
    if form.validate_on_submit():
        manager = Manager.query.filter_by(name = form.name.data, password = form.password.data).first()
        if manager is None:
            return render_template("login.html", form = form, message = "Wrong Credentials. Please Try Again.")
        else:
            session.permanent = True
            session["manager"] = manager.name
            return render_template("login.html", message = "Successfully Logged In!")
    return render_template("login.html", form = form)


@app.route("/logout")
def logout():
    if 'manager' in session:
        session.pop('manager')
    return redirect(url_for('homepage', _scheme='https', _external=True))


@app.route("/")
def homepage():
    return render_template("index.html")



@app.route('/about')
def about():
    """View About page"""
    return render_template('about.html')

@app.route('/detection')
def tracktransaction():
    """View About Transaction"""
    stmt1 = "Select * from eventtransaction ;"
    stmt2 = "SELECT COUNT(*) from eventtransaction ;"
    data_event = session_cassandra.execute(stmt1)
    count_event = session_cassandra.execute(stmt2)
    for i in count_event:
        count = i.count
    

    if( count == 0 ):
        return render_template('tracking.html',  message =  " No transaction !!!")  
    else:  
        ## View Transaction Event 
        return render_template('tracking.html', transaction = data_event , message = str(count))  
      

@app.route('/user')
def user():
   return render_template('user.html') 

@app.route('/research' , methods=["POST" , "GET"] )
def research():
    """View Transaction Latest"""
    
    if request.method == 'POST':
        ccid = request.form['ccid']
        
        stmt = " Select count(*) from eventtransaction where cc_num ='"+ str(ccid) + "';"
        count_event = session_cassandra.execute(stmt)
        for i in count_event:
            count = i.count

        if( count >= 10):
            stmt_data =  "  Select cc_num,trans_time,amt,category,merchant,trans_num  from eventtransaction where cc_num ='"+ str(ccid) + "' order by trans_time  DESC  limit 10 ; "
            data = session_cassandra.execute(stmt_data)
            df = pd.DataFrame([d for d in data])
        else:
            top10 = 10 - count 
            stmt_event =  "  Select cc_num,trans_time,amt,category,merchant,trans_num   from eventtransaction where cc_num ='"+ str(ccid) + "' order by trans_time  DESC  ; "
            stmt_history = "  Select cc_num,trans_time,amt,category,merchant,trans_num  from  view_top10_latest  where  cc_num = '"+ str(ccid) + "'  limit " + str(top10) + " ALLOW FILTERING   ;" 
            data_event = session_cassandra.execute(stmt_event)
            df_event  = pd.DataFrame([d for d in data_event])
            
            data_history = session_cassandra.execute(stmt_history)           
            df_history = pd.DataFrame([d for d in data_history])
            if( count == 0 ):
                df = df_history.sort_values(by='trans_time' , ascending=False)
                
            else:
                df_top10 = pd.DataFrame( np.vstack((df_event, df_history)) ,columns= df_event.columns )
                df = df_top10.sort_values(by='trans_time' , ascending=False)

        from plot.chart6 import chart6
        from plot.chart7 import chart7
        from plot.datatable import datatable


 

   
        chart6 = chart6(df)
        chart7 = chart7(df)
    
        datatable = datatable(df)

        js_resources = INLINE.render_js()
        css_resources = INLINE.render_css()

        chart6, div1 = components(chart6)
        chart7, div2 = components(chart7)
  

        render_template = flask.render_template('research.html',cc_id =ccid , datatable=datatable, 
                        script1=chart6, div1=div1,
                       script2= chart7, div2=div2, 
                      js_resources=js_resources, css_resources=css_resources)   
        return render_template




@app.route('/customerdetails/<int:cc_num>' , methods=["POST" , "GET"] )
def customer_details(cc_num):
    """View Customer Detail"""
    cc_id = cc_num
    stmt= " Select * from customer where cc_num = '"+ str(cc_id) + "';"
    customer = session_cassandra.execute(stmt)
    return  render_template('customerdetail.html' , customer = customer )


@app.route('/predict', methods=["POST" , "GET"] )
def predict():
         
    stmt1 =  "SELECT * FROM eventtransaction ;"
    stmt2 = "SELECT  * from customer ;"
    data_event = session_cassandra.execute(stmt1)
    df_event  = pd.DataFrame([d for d in data_event])
    if(len(df_event) == 0):
        return render_template('predict.html', message = "Not recive event transaction" )
    else:
    
        data_customer = session_cassandra.execute(stmt2)
        df_customer = pd.DataFrame([d for d in data_customer])

        df_transaction = get_feature(df_event, df_customer)
        result = predict_model(df_transaction)
        result_fraud  = result.loc[result['predict'] ==1 ]
        list_dict = []

        for index, row in list(result_fraud.iterrows()):
            list_dict.append(dict(row))
        return render_template('predict.html' , transaction = list_dict)



    
@app.route("/history")
def dashboard_history():
    stmt1 = "SELECT COUNT(*) from customer ;"
    stmt2 = "SELECT COUNT(*) from fraud_transaction ;"
    stmt3 = "SELECT COUNT(*) from non_fraud_transaction ;"
    count1 = session_cassandra.execute(stmt1) 
    count2 = session_cassandra.execute(stmt2)
    count3 = session_cassandra.execute(stmt3)
    for i in count1:
        dk1 = i.count
    for i in count2:
        dk2 = i.count
    for i in count3:
        dk3 = i.count
    if( dk1 == 0 or dk2 == 0 or dk3 == 0):
        return flask.render_template('dashboard.html' , message = "Not finish Processing !!!" )
    else:

    
        from plot.chart1 import chart1
        from plot.chart2 import chart2
        from plot.chart3 import chart3
        from plot.chart4 import chart4
        from plot.chart5 import chart5
    
    
        chart1 = chart1()
        chart2 = chart2()
        chart3 = chart3()
        chart4 = chart4()
        chart5 = chart5()
        js_resources = INLINE.render_js()
        css_resources = INLINE.render_css()


        chart1, div1 = components(chart1)
        chart2, div2 = components(chart2)
        chart3, div3 = components(chart3)
        chart4, div4 = components(chart4)
        chart5, div5 = components(chart5)
    

        render_template = flask.render_template('dashboard.html', script1=chart1, 
                      div1=div1, script2= chart2, div2=div2, script3 = chart3,
                      div3 = div3, script4=chart4, div4=div4,script5=chart5, div5=div5,
                      js_resources=js_resources, css_resources=css_resources)   
        return render_template

if __name__ == "__main__" :
    app.run(debug=True , host='0.0.0.0' , port=3000, threaded=True)







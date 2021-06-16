from flask import Flask, redirect, url_for,request, jsonify
import pandas as pds
import Protections
from threading import Thread
from flask import Flask, render_template
import logging

app = Flask(__name__,template_folder='templates')
app.config['EXPLAIN_TEMPLATE_LOADING'] = True

@app.route("/")
def home():
    return render_template('home.html')

@app.route("/<name>")
def user(name):
    return f"Hi I am taking input from user {name}!"

@app.route("/admin")
def admin():
    redirect(url_for("home"))

@app.route("/modelcalc", methods=['GET'])
def calc():
    if 'protection' in request.args:
        protection = int(request.args['protection'])
    else:
        return "Error: No id field provided. Please specify Protection=1 or Protection=2."
        
    if protection == 1:
        app.logger.info("Just an information for Protection 1") 
        objProtection1 = Protections.Protection()
        df_list = objProtection1.getOutputProtection1()
        columnNames = df_list.columns.values
        temp = df_list.to_dict('records')
        return render_template('record.html', records=temp, colnames=columnNames,protection='Protection 1') 
       
    if protection == 2:
        #print(app.config)
        app.logger.info("Just an information for Protection 2")
        objProtection1 = Protections.Protection()
        df_list = objProtection1.getOutputProtection2()
        columnNames = df_list.columns.values
        temp = df_list.to_dict('records')
        return render_template('record.html', records=temp, colnames=columnNames,protection='Protection 2') 
     
    
if __name__ =="__main__":
    app.run(threaded = True)
    logging.basicConfig(filename="logPOC.log", 
					format='%(asctime)s %(message)s', 
					filemode='w') 
    logger=logging.getLogger()
    logger.setLevel(logging.DEBUG) 


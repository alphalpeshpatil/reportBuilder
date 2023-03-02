import psycopg2
from flask import Flask,jsonify,render_template, session, abort, redirect, request
import requests

connection = psycopg2.connect(user="postgres",password="root",host="localhost",port="5433",database="postgres")
cursor = connection.cursor()

app = Flask(__name__)

@app.route('/storeData',methods=['GET',"POST"])
def storeData():
    return "dataStored"

@app.route('/createReport',methods=['GET',"POST"])
def createReport():
    return "dataStored"



if __name__ == '__main__':
    app.run(debug=True)

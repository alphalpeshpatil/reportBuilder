import psycopg2
import reportBuilder
from flask import Flask,jsonify,render_template, session, abort, redirect, request
import requests

connection = psycopg2.connect(user="postgres",password="root",host="localhost",port="5433",database="postgres")
cursor = connection.cursor()

app = Flask(__name__)

@app.route('/getData',methods=['GET',"POST"])
def getData():
    _req=request.form
    tableName=_req['tableName']
    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (tableName,))

# Fetch all the rows of the query result
    rows = cursor.fetchall()

# Create a dictionary with column names as keys
    column_dict = {row[0]: None for row in rows}

# Iterate over the dictionary to get values from the user
    for key in column_dict:
        value =_req[key].format(key)
        column_dict[key] = value
    result=reportBuilder.dataOfTable(tableName,column_dict)
    return result

@app.route('/viewDataByMultipleAngles',methods=['GET',"POST"])
def viewDataByMultipleAngles():
    _req=request.form
    tableName=_req['tableName']
    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (tableName,))

# Fetch all the rows of the query result
    rows = cursor.fetchall()

# Create a dictionary with column names as keys
    column_dict = {row[0]: None for row in rows}

# Iterate over the dictionary to get values from the user
    for key in column_dict:
        value =_req[key].format(key)
        column_dict[key] = value
    sum1=_req['sum1']
    sum2=_req['sum2']
    groupBy=_req['group_by']
    result=reportBuilder.viewData(tableName,column_dict,groupBy,sum1,sum2)
    return result

if __name__ == '__main__':
    app.run(debug=True)

import psycopg2
import reportBuilder
from flask import Flask,jsonify,render_template, session, abort, redirect, request
import requests

connection = psycopg2.connect(user="postgres",password="root",host="localhost",port="5433",database="postgres")
cursor = connection.cursor()

app = Flask(__name__)

@app.route('/showTables',methods=['GET','POST'])
def showTables():
    dict={}
    query=("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    cursor.execute(query)
    result=cursor.fetchall()
    i=1
    for row in result:
        dict[i]=row
        i=i+1
    return dict
    
@app.route('/getData',methods=['GET',"POST"])
def getData():
    _req=request.json
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
        
    var=str(tableName)
    
# Build the SELECT statement based on the dictionary values
    select_stmt = "SELECT "
    column_names = [key for key in column_dict.keys() if column_dict[key]]
    select_stmt += ", ".join(column_names)
    select_stmt += " FROM " + var
    
    list=[]
    
    # Execute the SELECT statement and fetch the results
    cursor.execute(select_stmt)
    selected_data = cursor.fetchall()
    for row in selected_data:
        ans={}
        result_dict = {}
        for i in range(len(column_names)):
            result_dict[column_names[i]] = row[i]
        ans=result_dict
        list.append(ans)
    # result=reportBuilder.dataOfTable(tableName,column_dict)
    return jsonify(list)

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
    date=_req['date']
    start_date=_req['sdate']
    end_date=_req['edate']
    groupBy=_req['group_by']
    result=reportBuilder.viewData(tableName,column_dict,groupBy,sum1,date,start_date,end_date)
    return result

if __name__ == '__main__':
    app.run(debug=True,port=7010)

import psycopg2
import reportBuilder
import curd
from flask_cors import CORS

from flask import Flask,jsonify,render_template, session, abort, redirect, request
import requests

connection = psycopg2.connect(user="postgres",password="root",host="localhost",port="5433",database="postgres")
cursor = connection.cursor()

app = Flask(__name__)
CORS(app,supports_credentials=True,origins=['*'])

@app.route('/api/test',methods=['GET'])
def default():
    return "connected"

@app.route('/api/showTables',methods=['GET'])
def showTables():
    query=("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    result=curd.dbTransactionSelect(query)
    return jsonify(result)

@app.route('/api/getColumnsOfTable',methods=['POST'])
def getColumnsOfTable():
    _req=request.json
    tableName=_req['tableName']
    query = "SELECT column_name FROM information_schema.columns WHERE table_name = '{}';".format(tableName)
    result=curd.dbTransactionSelect(query)
    return jsonify(result)

def data(tableName,column_dict):
    select_stmt = "SELECT "
    column_names = [key for key in column_dict.keys() if column_dict[key]]
    select_stmt += ", ".join(column_names)
    select_stmt += " FROM " + str(tableName)
    
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
    return (list)

@app.route('/api/selectMultipleTablesWithTheirColumns',methods=['GET','POST'])
def selectTables():
    _req=request.json
    query=("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    cursor.execute(query)
    rows=cursor.fetchall()
    
    selected_tables={row[0]:None for row in rows}
    ans=[]
    for keys in range(len(selected_tables)):
        table_name = list(selected_tables.keys())[keys]
        # fetching tableName by index, so first i converted it inot a list of keys as direct using dict[keys] i am not able to fetch a table name at particular index from postman
        table_value = _req.get(table_name)
        if table_value:
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (table_name,))
            rows = cursor.fetchall()
            column_dict = {row[0]: None for row in rows}
            for column_name in column_dict:
                column_value = table_value.get(column_name)
                if column_value:
                    column_dict[column_name] = column_value
            ans.append(data(table_name, column_dict))
    return jsonify(ans)
     # now selected_tables contains the key with value of tables which is selected by user.
   
@app.route('/api/getData',methods=['GET',"POST"])
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
    
    select_stmt = "SELECT "
    column_names = [key for key in column_dict.keys() if column_dict[key]]
    select_stmt += ", ".join(column_names)
    select_stmt += " FROM " + tableName

    conditions = _req.get("conditions")
    if conditions:
        for condition in conditions:
            logicalOpe = condition.get("logicalOpe")
            operator = condition.get("operator")
            inputColumn = condition.get("inputColumn")
            values = condition.get("values")
            low = condition.get("low")
            high = condition.get("high")

            if logicalOpe:
                    if logicalOpe=="AND":
                        if operator=="IN":
                            select_stmt += " AND {} IN ({})".format(inputColumn, ", ".join(values))
                        elif operator=="BETWEEN":
                            select_stmt += " AND {} BETWEEN {} AND {}".format(inputColumn, ", ".join(low),", ".joins(high))
                        elif operator=="NOT IN":
                            select_stmt += " AND {} NOT IN ({})".format(inputColumn, ", ".join(value))
                        elif operator=="NOT BETWEEN":
                            select_stmt += " AND {} NOT BETWEEN {} AND {}".format(inputColumn, ", ".join(low),", ".joins(high))
                    elif logicalOpe=="OR":
                        if operator=="IN":
                            select_stmt += " OR {} IN ({})".format(inputColumn, ", ".join(values))
                        elif operator=="BETWEEN":
                            select_stmt += " AND {} BETWEEN {} AND {}".format(inputColumn, ", ".join(low),", ".joins(high))
                        elif operator=="NOT IN":
                            select_stmt += " OR {} NOT IN ({})".format(inputColumn, ", ".join(value))
                        elif operator=="NOT BETWEEN":
                            select_stmt += " AND {} NOT BETWEEN {} AND {}".format(inputColumn, ", ".join(low),", ".joins(high))
                    elif logicalOpe=="NOT":
                        if operator=="IN":
                            select_stmt += " NOT {} IN ({})".format(inputColumn, ", ".join(values))
                        elif operator=="BETWEEN":
                            select_stmt += " AND {} BETWEEN {} AND {}".format(inputColumn, ", ".join(low),", ".joins(high))
                        elif operator=="NOT IN":
                            select_stmt += " NOT {} NOT IN ({})".format(inputColumn, ", ".join(value))
                        elif operator=="NOT BETWEEN":
                            select_stmt += " AND {} NOT BETWEEN {} AND {}".format(inputColumn, ", ".join(low),", ".joins(high))
            else:
                if operator == "IN":
                    select_stmt += " WHERE {} IN ({})".format(inputColumn, ", ".join(values))
                elif operator == "BETWEEN":
                    select_stmt += " WHERE {} BETWEEN {} AND {}".format(inputColumn, low, high)
                elif operator == "NOT IN":
                    select_stmt += " WHERE {} NOT IN ({})".format(inputColumn, ", ".join(values))
                elif operator == "NOT BETWEEN":
                    select_stmt += " WHERE {} NOT BETWEEN {} AND {}".format(inputColumn, low, high)
    print(select_stmt)
    # Execute the SELECT statement and fetch the results
    ans=curd.dbTransactionSelect(select_stmt)
    return jsonify(ans)

@app.route('/api/viewDataByMultipleAngles',methods=['GET',"POST"])
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

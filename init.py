import psycopg2
import reportBuilder
import curd
from flask_cors import CORS

from flask import Flask,jsonify,render_template, session, abort, redirect, request
import requests

connection = psycopg2.connect(user="postgres",password="root",host="localhost",port="5433",database="postgres")
cursor = connection.cursor()

app = Flask(__name__)
# CORS(app,supports_credentials=True,origins=['*'])
CORS(app,supports_credentials=True,origins=['http://localhost:3000'])

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

def data(tableName,listOfColumns,conditions):
    select_stmt = "SELECT "
    column_names =listOfColumns
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
    _req = request.json
    ans = []
    selected_tables = _req.get("tables")
    for table in selected_tables:
        table_name = table.get("name")
        if table_name:
            column_dict = table.get("columnNames") # fetch column_dict based on selected table
            listOfColumns = []
            for obj in column_dict:
                value = obj.get("column_name")
                listOfColumns.append(value)
            select_stmt = "SELECT "
            column_names = listOfColumns
            select_stmt += ", ".join(column_names)
            select_stmt += " FROM " + table_name
            conditions = table.get("conditions")
            # do something with listOfColumns and conditions
            if conditions:
                for condition in conditions:
                    logicalOpe = condition.get("logicalOpe")
                    operator = condition.get("operator")
                    inputColumn = condition.get("inputColumn")
                    values = condition.get("values")
                    low = condition.get("low")
                    high = condition.get("high")
                    result = None
                    if operator:
                        ans1 = curd.getDataType(table_name, inputColumn)
                        for col in ans1:
                            result = col['data_type']
                            break
                        print("data type is----->>>>")
                        print(result)
                    if logicalOpe:
                        if logicalOpe=="AND":
                            if operator == "Not Ends With":
                                select_stmt += " AND {} NOT LIKE '%{}'".format(inputColumn, "".join(values))
                            elif operator == "Not Starts With":
                                select_stmt += " AND {} NOT LIKE '{}%'".format(inputColumn, "".join(values))
                            elif operator == "Not LIKE":
                                select_stmt += " AND {} NOT LIKE '%{}%'".format(inputColumn, "".join(values))
                            elif operator == "Ends with":
                                select_stmt += " AND {} LIKE '%{}'".format(inputColumn, "".join(values))
                            elif operator == "Starts with":
                                select_stmt += " AND {} LIKE '{}%'".format(inputColumn, "".join(values))
                            elif operator == "LIKE":
                                select_stmt += " AND {} LIKE '%{}%'".format(inputColumn, "".join(values))
                            elif operator=="=": 
                                select_stmt+=" AND {} = {}".format(inputColumn, ", ".join(values))
                            elif operator=="!=":
                                select_stmt+=" AND {} != {}".format(inputColumn, ", ".join(values))
                            elif operator=="<" and result!='character varying':
                                select_stmt+=" AND {} < {}".format(inputColumn, ", ".join(values))
                            elif operator==">" and result!='character varying':
                                select_stmt+=" AND {} > {}".format(inputColumn, ", ".join(values))
                            elif operator==">=" and result!='character varying':
                                select_stmt+=" AND {} >= {}".format(inputColumn, ", ".join(values))
                            elif operator=="<=" and result!='character varying':
                                select_stmt+=" AND {} <= {}".format(inputColumn, ", ".join(values))
                            elif operator=="IN":
                                select_stmt += " AND {} IN ({})".format(inputColumn, ", ".join(values))
                            elif operator=="BETWEEN":
                                select_stmt += " AND {} BETWEEN {} AND {}".format(inputColumn, ", ".join(low),", ".joins(high))
                            elif operator=="NOT IN":
                                select_stmt += " AND {} NOT IN ({})".format(inputColumn, ", ".join(value))
                            elif operator=="NOT BETWEEN":
                                select_stmt += " AND {} NOT BETWEEN {} AND {}".format(inputColumn, ", ".join(low),", ".joins(high))
                        elif logicalOpe=="OR":
                            if operator == "Not Ends With":
                                select_stmt += " AND {} NOT LIKE '%{}'".format(inputColumn, "".join(values))
                            elif operator == "Not Starts With":
                                select_stmt += " AND {} NOT LIKE '{}%'".format(inputColumn, "".join(values))
                            elif operator == "Not LIKE":
                                select_stmt += " AND {} NOT LIKE '%{}%'".format(inputColumn, "".join(values))
                            elif operator == "Ends with":
                                select_stmt += " AND {} LIKE '%{}'".format(inputColumn, "".join(values))
                            elif operator == "Starts with":
                                select_stmt += " AND {} LIKE '{}%'".format(inputColumn, "".join(values))
                            elif operator == "LIKE":
                                select_stmt += " AND {} LIKE '%{}%'".format(inputColumn, "".join(values))
                            elif operator=="=":
                                select_stmt+=" AND {} = {}".format(inputColumn, ", ".join(values))
                            elif operator=="!=":
                                select_stmt+=" AND {} != {}".format(inputColumn, ", ".join(values))
                            elif operator=="<" and result!='character varying':
                                select_stmt+=" AND {} < {}".format(inputColumn, ", ".join(values))
                            elif operator==">" and result!='character varying':
                                select_stmt+=" AND {} > {}".format(inputColumn, ", ".join(values))
                            elif operator==">=" and result!='character varying':
                                select_stmt+=" AND {} >= {}".format(inputColumn, ", ".join(values))
                            elif operator=="<=" and result!='character varying':
                                select_stmt+=" AND {} <= {}".format(inputColumn, ", ".join(values))
                            elif operator=="IN":
                                select_stmt += " OR {} IN ({})".format(inputColumn, ", ".join(values))
                            elif operator=="BETWEEN":
                                select_stmt += " AND {} BETWEEN {} AND {}".format(inputColumn, ", ".join(low),", ".joins(high))
                            elif operator=="NOT IN":
                                select_stmt += " OR {} NOT IN ({})".format(inputColumn, ", ".join(value))
                            elif operator=="NOT BETWEEN":
                                select_stmt += " AND {} NOT BETWEEN {} AND {}".format(inputColumn, ", ".join(low),", ".joins(high))
                        elif logicalOpe=="NOT":
                            if operator == "Not Ends With":
                                select_stmt += " AND {} NOT LIKE '%{}'".format(inputColumn, "".join(values))
                            elif operator == "Not Starts With":
                                select_stmt += " AND {} NOT LIKE '{}%'".format(inputColumn, "".join(values))
                            elif operator == "Not LIKE":
                                select_stmt += " AND {} NOT LIKE '%{}%'".format(inputColumn, "".join(values))
                            elif operator == "Ends with":
                                select_stmt += " AND {} LIKE '%{}'".format(inputColumn, "".join(values))
                            elif operator == "Starts with":
                                select_stmt += " AND {} LIKE '{}%'".format(inputColumn, "".join(values))
                            elif operator == "LIKE":
                                select_stmt += " AND {} LIKE '%{}%'".format(inputColumn, "".join(values))
                            elif operator=="=":
                                select_stmt+=" AND {} = {}".format(inputColumn, ", ".join(values))
                            elif operator=="!=":
                                select_stmt+=" AND {} != {}".format(inputColumn, ", ".join(values))
                            elif operator=="<" and result!='character varying':
                                select_stmt+=" AND {} < {}".format(inputColumn, ", ".join(values))
                            elif operator==">" and result!='character varying':
                                select_stmt+=" AND {} > {}".format(inputColumn, ", ".join(values))
                            elif operator==">=" and result!='character varying':
                                select_stmt+=" AND {} >= {}".format(inputColumn, ", ".join(values))
                            elif operator=="<=" and result!='character varying':
                                select_stmt+=" AND {} <= {}".format(inputColumn, ", ".join(values))
                            elif operator=="IN":
                                select_stmt += " NOT {} IN ({})".format(inputColumn, ", ".join(values))
                            elif operator=="BETWEEN":
                                select_stmt += " AND {} BETWEEN {} AND {}".format(inputColumn, ", ".join(low),", ".joins(high))
                            elif operator=="NOT IN":
                                select_stmt += " NOT {} NOT IN ({})".format(inputColumn, ", ".join(value))
                            elif operator=="NOT BETWEEN":
                                select_stmt += " AND {} NOT BETWEEN {} AND {}".format(inputColumn, ", ".join(low),", ".joins(high))
                    else:
                        if operator == "Not Ends With":
                            select_stmt += " WHERE {} NOT LIKE '%{}'".format(inputColumn, "".join(values))
                        elif operator == "Not Starts With":
                            select_stmt += " WHERE {} NOT LIKE '{}%'".format(inputColumn, "".join(values))
                        elif operator == "Not LIKE":
                            select_stmt += " WHERE {} NOT LIKE '%{}%'".format(inputColumn, "".join(values))
                        elif operator == "Ends with":
                            select_stmt += " WHERE {} LIKE '%{}'".format(inputColumn, "".join(values))
                        elif operator == "Starts with":
                            select_stmt += " WHERE {} LIKE '{}%'".format(inputColumn, "".join(values))
                        elif operator == "LIKE":
                            select_stmt += " WHERE {} LIKE '%{}%'".format(inputColumn, "".join(values))
                        elif operator=="=":
                            select_stmt+=" WHERE {} = {}".format(inputColumn, ", ".join(values))
                        elif operator=="!=":
                            select_stmt+=" WHERE {} != {}".format(inputColumn, ", ".join(values))
                        elif operator=="<" and result!='character varying':
                            select_stmt+=" WHERE {} < {}".format(inputColumn, ", ".join(values))
                        elif operator==">" and result!='character varying':
                            select_stmt+=" WHERE {} > {}".format(inputColumn, ", ".join(values))
                        elif operator==">=" and result!='character varying':
                            select_stmt+=" WHERE {} >= {}".format(inputColumn, ", ".join(values))
                        elif operator=="<=" and result!='character varying':
                            select_stmt+=" WHERE {} <= {}".format(inputColumn, ", ".join(values))
                        elif operator == "IN":
                            select_stmt += " WHERE {} IN ({})".format(inputColumn, ", ".join(values))
                        elif operator == "BETWEEN":
                            select_stmt += " WHERE {} BETWEEN {} AND {}".format(inputColumn, low, high)
                        elif operator == "NOT IN":
                            select_stmt += " WHERE {} NOT IN ({})".format(inputColumn, ", ".join(values))
                        elif operator == "NOT BETWEEN":
                            select_stmt += " WHERE {} NOT BETWEEN {} AND {}".format(inputColumn, low, high)
            result2=curd.dbTransactionSelect(select_stmt)
            print(select_stmt)
            ans.append(result2)
    return jsonify(ans)
     # now selected_tables contains the key with value of tables which is selected by user.

@app.route('/api/getReport',methods=['POST'])
def getRepot():
    select_stmt=""
    _req=request.json
    reportname=_req["reportname"]
    query="select querystr from report1 where reportname ='{}'".format(reportname)
    result=curd.dbTransactionSelect(query)
    if(result=="No data Found"):
        return jsonify("This report does not exist")
    else:
        select_stmt = result[0]["querystr"]
        result2=curd.dbTransactionSelect(select_stmt)
        return jsonify(result2)

@app.route('/api/getData',methods=["POST"])
def getData():
    _req=request.json
    reportName=_req['reportName']
    tableName=_req['tableName']
    column_dict = _req.get("columnNames")
    listOfColumns=[]
# Iterate over the dictionary to get values from the user
    for obj in column_dict:
        value = obj.get("column_name")
        listOfColumns.append(value)
        
    select_stmt = "SELECT "
    column_names = listOfColumns
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
            result = None
            ans1 = curd.getDataType(tableName, inputColumn)
            for col in ans1:
                result = col['data_type']
                break
            print("data type is----->>>>")
            print(result)
            if logicalOpe:
                    if logicalOpe=="AND":
                        if operator == "Not Ends With":
                            select_stmt += " AND {} NOT LIKE '%{}'".format(inputColumn, "".join(values))
                        elif operator == "Not Starts With":
                            select_stmt += " AND {} NOT LIKE '{}%'".format(inputColumn, "".join(values))
                        elif operator == "Not LIKE":
                            select_stmt += " AND {} NOT LIKE '%{}%'".format(inputColumn, "".join(values))
                        elif operator == "Ends with":
                            select_stmt += " AND {} LIKE '%{}'".format(inputColumn, "".join(values))
                        elif operator == "Starts with":
                            select_stmt += " AND {} LIKE '{}%'".format(inputColumn, "".join(values))
                        elif operator == "LIKE":
                            select_stmt += " AND {} LIKE '%{}%'".format(inputColumn, "".join(values))
                        elif operator=="=": 
                            select_stmt+=" AND {} = {}".format(inputColumn, ", ".join(values))
                        elif operator=="!=":
                            select_stmt+=" AND {} != {}".format(inputColumn, ", ".join(values))
                        elif operator=="<" and result!='character varying':
                            select_stmt+=" AND {} < {}".format(inputColumn, ", ".join(values))
                        elif operator==">" and result!='character varying':
                            select_stmt+=" AND {} > {}".format(inputColumn, ", ".join(values))
                        elif operator==">=" and result!='character varying':
                            select_stmt+=" AND {} >= {}".format(inputColumn, ", ".join(values))
                        elif operator=="<=" and result!='character varying':
                            select_stmt+=" AND {} <= {}".format(inputColumn, ", ".join(values))
                        elif operator=="IN":
                            select_stmt += " AND {} IN ({})".format(inputColumn, ", ".join(values))
                        elif operator=="BETWEEN":
                            select_stmt += " AND {} BETWEEN {} AND {}".format(inputColumn, ", ".join(low),", ".joins(high))
                        elif operator=="NOT IN":
                            select_stmt += " AND {} NOT IN ({})".format(inputColumn, ", ".join(value))
                        elif operator=="NOT BETWEEN":
                            select_stmt += " AND {} NOT BETWEEN {} AND {}".format(inputColumn, ", ".join(low),", ".joins(high))
                    elif logicalOpe=="OR":
                        if operator == "Not Ends With":
                            select_stmt += " AND {} NOT LIKE '%{}'".format(inputColumn, "".join(values))
                        elif operator == "Not Starts With":
                            select_stmt += " AND {} NOT LIKE '{}%'".format(inputColumn, "".join(values))
                        elif operator == "Not LIKE":
                            select_stmt += " AND {} NOT LIKE '%{}%'".format(inputColumn, "".join(values))
                        elif operator == "Ends with":
                            select_stmt += " AND {} LIKE '%{}'".format(inputColumn, "".join(values))
                        elif operator == "Starts with":
                            select_stmt += " AND {} LIKE '{}%'".format(inputColumn, "".join(values))
                        elif operator == "LIKE":
                            select_stmt += " AND {} LIKE '%{}%'".format(inputColumn, "".join(values))
                        elif operator=="=":
                            select_stmt+=" AND {} = {}".format(inputColumn, ", ".join(values))
                        elif operator=="!=":
                            select_stmt+=" AND {} != {}".format(inputColumn, ", ".join(values))
                        elif operator=="<" and result!='character varying':
                            select_stmt+=" AND {} < {}".format(inputColumn, ", ".join(values))
                        elif operator==">" and result!='character varying':
                            select_stmt+=" AND {} > {}".format(inputColumn, ", ".join(values))
                        elif operator==">=" and result!='character varying':
                            select_stmt+=" AND {} >= {}".format(inputColumn, ", ".join(values))
                        elif operator=="<=" and result!='character varying':
                            select_stmt+=" AND {} <= {}".format(inputColumn, ", ".join(values))
                        elif operator=="IN":
                            select_stmt += " OR {} IN ({})".format(inputColumn, ", ".join(values))
                        elif operator=="BETWEEN":
                            select_stmt += " AND {} BETWEEN {} AND {}".format(inputColumn, ", ".join(low),", ".joins(high))
                        elif operator=="NOT IN":
                            select_stmt += " OR {} NOT IN ({})".format(inputColumn, ", ".join(value))
                        elif operator=="NOT BETWEEN":
                            select_stmt += " AND {} NOT BETWEEN {} AND {}".format(inputColumn, ", ".join(low),", ".joins(high))
                    elif logicalOpe=="NOT":
                        if operator == "Not Ends With":
                            select_stmt += " AND {} NOT LIKE '%{}'".format(inputColumn, "".join(values))
                        elif operator == "Not Starts With":
                            select_stmt += " AND {} NOT LIKE '{}%'".format(inputColumn, "".join(values))
                        elif operator == "Not LIKE":
                            select_stmt += " AND {} NOT LIKE '%{}%'".format(inputColumn, "".join(values))
                        elif operator == "Ends with":
                            select_stmt += " AND {} LIKE '%{}'".format(inputColumn, "".join(values))
                        elif operator == "Starts with":
                            select_stmt += " AND {} LIKE '{}%'".format(inputColumn, "".join(values))
                        elif operator == "LIKE":
                            select_stmt += " AND {} LIKE '%{}%'".format(inputColumn, "".join(values))
                        elif operator=="=":
                            select_stmt+=" AND {} = {}".format(inputColumn, ", ".join(values))
                        elif operator=="!=":
                            select_stmt+=" AND {} != {}".format(inputColumn, ", ".join(values))
                        elif operator=="<" and result!='character varying':
                            select_stmt+=" AND {} < {}".format(inputColumn, ", ".join(values))
                        elif operator==">" and result!='character varying':
                            select_stmt+=" AND {} > {}".format(inputColumn, ", ".join(values))
                        elif operator==">=" and result!='character varying':
                            select_stmt+=" AND {} >= {}".format(inputColumn, ", ".join(values))
                        elif operator=="<=" and result!='character varying':
                            select_stmt+=" AND {} <= {}".format(inputColumn, ", ".join(values))
                        elif operator=="IN":
                            select_stmt += " NOT {} IN ({})".format(inputColumn, ", ".join(values))
                        elif operator=="BETWEEN":
                            select_stmt += " AND {} BETWEEN {} AND {}".format(inputColumn, ", ".join(low),", ".joins(high))
                        elif operator=="NOT IN":
                            select_stmt += " NOT {} NOT IN ({})".format(inputColumn, ", ".join(value))
                        elif operator=="NOT BETWEEN":
                            select_stmt += " AND {} NOT BETWEEN {} AND {}".format(inputColumn, ", ".join(low),", ".joins(high))
            else:
                if operator == "Not Ends With":
                    select_stmt += " WHERE {} NOT LIKE '%{}'".format(inputColumn, "".join(values))
                elif operator == "Not Starts With":
                    select_stmt += " WHERE {} NOT LIKE '{}%'".format(inputColumn, "".join(values))
                elif operator == "Not LIKE":
                    select_stmt += " WHERE {} NOT LIKE '%{}%'".format(inputColumn, "".join(values))
                elif operator == "Ends with":
                    select_stmt += " WHERE {} LIKE '%{}'".format(inputColumn, "".join(values))
                elif operator == "Starts with":
                    select_stmt += " WHERE {} LIKE '{}%'".format(inputColumn, "".join(values))
                elif operator == "LIKE":
                    select_stmt += " WHERE {} LIKE '%{}%'".format(inputColumn, "".join(values))
                elif operator=="=":
                    select_stmt+=" WHERE {} = {}".format(inputColumn, ", ".join(values))
                elif operator=="!=":
                    select_stmt+=" WHERE {} != {}".format(inputColumn, ", ".join(values))
                elif operator=="<" and result!='character varying':
                    select_stmt+=" WHERE {} < {}".format(inputColumn, ", ".join(values))
                elif operator==">" and result!='character varying':
                    select_stmt+=" WHERE {} > {}".format(inputColumn, ", ".join(values))
                elif operator==">=" and result!='character varying':
                    select_stmt+=" WHERE {} >= {}".format(inputColumn, ", ".join(values))
                elif operator=="<=" and result!='character varying':
                    select_stmt+=" WHERE {} <= {}".format(inputColumn, ", ".join(values))
                elif operator == "IN":
                    select_stmt += " WHERE {} IN ({})".format(inputColumn, ", ".join(values))
                elif operator == "BETWEEN":
                    select_stmt += " WHERE {} BETWEEN {} AND {}".format(inputColumn, low, high)
                elif operator == "NOT IN":
                    select_stmt += " WHERE {} NOT IN ({})".format(inputColumn, ", ".join(values))
                elif operator == "NOT BETWEEN":
                    select_stmt += " WHERE {} NOT BETWEEN {} AND {}".format(inputColumn, low, high)
    result2=curd.dbTransactionSelect(select_stmt)
    print(select_stmt)
    # select_stmt=select_stmt.replace("'",'"')
    # print(select_stmt)
    
    # query = 'INSERT INTO report1 (reportname, querystr) VALUES ("'+str(reportName)+'","'+str(select_stmt)+'")'
    # print("----------------->>>>>>>>>>>>>>>")
    # print(query)
    # cursor.execute("SELECT * FROM report1 WHERE reportname = %s", (reportName,))
    # row=cursor.fetchone()
    # if row is not None and row[0] is not None:
    #     # Accessing the first element of the row
    #     ans = row[0]
    #     sql_update_query = """Update report1 set reportname = %s, refreshtoken=%s,time=%s where useremail = %s"""
    #     cursor.execute(sql_update_query,(excessToken,refreshToken,expirationTime,ans))
    #     connection.commit()
    #     count = cursor.rowcount
    #     print(count, "Record Updated successfully ")

    # curd.dbTransactionSelect(queryCheck)
    # to store query and report in database report1.
    checkQuery=("SELECT reportname FROM report1 WHERE reportname = '{}'".format(reportName))
    resultOfCheck=curd.dbTransactionSelect(checkQuery)
    if resultOfCheck !="No data Found":
        return jsonify("report already exist")
    else:
        try:
            query = "INSERT INTO report1 (reportname, querystr) VALUES ('"+str(reportName)+"',$$"+select_stmt+"$$)"
            print(query)
            # sql_where=(reportName,select_stmt)
            # cursor.execute(sql,sql_where)
            # connection.commit()
            # Execute the SELECT statement and fetch the results
            curd.dbTransactionIUD(query)
        except Exception as error:
            print(error)
        return jsonify(result2)

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

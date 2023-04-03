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

# @app.route('/api/getColumnsOfTable',methods=['POST'])
# def getColumnsOfTable():
#     _req=request.json
#     selected_tables = _req.get("table_name")
#     for table in selected_tables:
#         table_name = table.get("table")
#         if table_name:
#             query = "SELECT column_name FROM information_schema.columns WHERE table_name = '{}';".format(table_name)
#             result=curd.dbTransactionSelect(query)
#     return jsonify(result)

@app.route('/api/getColumnsOfTable', methods=['POST'])
def getColumnsOfTable():
    _req = request.json
    selected_tables = _req.get("table_name")
    result = []
    for table in selected_tables:
        table_name = table.get("table")
        if table_name:
            query = "SELECT column_name FROM information_schema.columns WHERE table_name = '{}';".format(table_name)
            columns = curd.dbTransactionSelect(query)
            if columns:
                for col in columns:
                    col["column_name"] = table_name + "." + col["column_name"] #concatinated the column name...
                result.append(columns)
    return jsonify(result)

@app.route('/api/showConditionsList',methods=["GET"])
def showConditionList():
    result=[]
    tempdict={}
    tempdict["condition"]="Not Ends With"
    result.append(tempdict.copy())  # copy the dictionary before appending to the list
    tempdict.clear()
    tempdict["condition"]="Not Starts With"
    result.append(tempdict.copy())  # copy the dictionary before appending to the list
    tempdict.clear()
    tempdict["condition"]="Not LIKE"
    result.append(tempdict.copy())  # copy the dictionary before appending to the list
    tempdict.clear()
    tempdict["condition"]="Ends with"
    result.append(tempdict.copy())  # copy the dictionary before appending to the list
    tempdict.clear()
    tempdict["condition"]="Starts with"
    result.append(tempdict.copy())  # copy the dictionary before appending to the list
    tempdict.clear()
    tempdict["condition"]="LIKE"
    result.append(tempdict.copy())  # copy the dictionary before appending to the list
    tempdict.clear()
    tempdict["condition"]="="
    result.append(tempdict.copy())  # copy the dictionary before appending to the list
    tempdict.clear()
    tempdict["condition"]="!="
    result.append(tempdict.copy())  # copy the dictionary before appending to the list
    tempdict.clear()
    tempdict["condition"]=">"
    result.append(tempdict.copy())  # copy the dictionary before appending to the list
    tempdict.clear()
    tempdict["condition"]="<"
    result.append(tempdict.copy())  # copy the dictionary before appending to the list
    tempdict.clear()
    tempdict["condition"]=">="
    result.append(tempdict.copy())  # copy the dictionary before appending to the list
    tempdict.clear()
    tempdict["condition"]="<="
    result.append(tempdict.copy())  # copy the dictionary before appending to the list
    tempdict.clear()
    tempdict["condition"]="IN"
    result.append(tempdict.copy())  # copy the dictionary before appending to the list
    tempdict.clear()
    tempdict["condition"]="BETWEEN"
    result.append(tempdict.copy())  # copy the dictionary before appending to the list
    tempdict.clear()
    tempdict["condition"]="NOT IN"
    result.append(tempdict.copy())  # copy the dictionary before appending to the list
    tempdict.clear()
    tempdict["condition"]="NOT BETWEEN"
    result.append(tempdict.copy())  # copy the dictionary before appending to the list
    tempdict.clear()
    tempdict["condition"]="Group By"
    result.append(tempdict.copy())  # copy the dictionary before appending to the list
    tempdict.clear()
    tempdict["condition"]="Order By"
    result.append(tempdict.copy())  # copy the dictionary before appending to the list
    tempdict.clear()
    return jsonify(result)

@app.route('/api/showGroupingFuncions',methods=["GET"])
def showGroupingFuncions():
    result=[]
    tempdict={}
    tempdict["groupingFunction"]="sum"
    result.append(tempdict.copy()) 
    tempdict.clear()
    tempdict["condition"]="avg"
    result.append(tempdict.copy())  
    tempdict.clear()
    tempdict["condition"]="max"
    result.append(tempdict.copy())  
    tempdict.clear()
    tempdict["condition"]="min"
    result.append(tempdict.copy())  
    tempdict.clear()
    tempdict["condition"]="count"
    result.append(tempdict.copy())  
    tempdict.clear()
    return result
    
@app.route('/api/showOrderByClause',methods=["GET"])
def showOrderByClause():
    result=[]
    tempdict={}
    tempdict["groupingFunction"]="ascending"
    result.append(tempdict.copy()) 
    tempdict.clear()
    tempdict["condition"]="descending"
    result.append(tempdict.copy())  
    tempdict.clear()
    return result

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


def check_common_columns(selected_tables, select_stmt):
    # Get column names for each table
    column_lists = [set(tuple(col.items()) for col in table.get("columnNames", [])) for table in selected_tables]

    # Find common columns between first two tables
    common_columns = column_lists[0].intersection(column_lists[1])

    # Find common columns between subsequent tables
    for columns in column_lists[2:]:
        common_columns = common_columns.intersection(columns)

    # Construct the WHERE clause based on the common columns and table names
    where_conditions = []
    for i in range(len(selected_tables) - 1):
        table1 = selected_tables[i].get("tableName")
        table2 = selected_tables[i + 1].get("tableName")
        common_cols = list(common_columns.intersection(column_lists[i]).intersection(column_lists[i+1]))
        for col in common_cols:
            where_conditions.append(f"{table1}.{col} = {table2}.{col}")
    where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

    # Append the WHERE clause to the SELECT statement
    select_stmt += " " + where_clause

    if len(common_columns)==len(selected_tables)-1:
        return select_stmt
    else:
        return 0

@app.route('/api/selectMultipleTablesWithTheirColumns',methods=['GET','POST'])
def selectTables():
    _req = request.json
    ans = []
    flag=0
    
    # new code --------------------------------------------------
    commonColumnsCheck=0
    count=0
    reportName=_req['reportName']
    selected_tables = _req.get("tables")
    tableList=[]
    select_stmt = "SELECT "
    for table in selected_tables:
        table_name=table.get("name")
        if table_name:
            count=count+1
            tableListColumns={}
            
            tableListColumns["name"]=table_name
            tableListColumns["columnNames"]=table.get("columnNames")
            tableList.append(tableListColumns)
            
            column_dict = table.get("columnNames") # fetch column_dict based on selected table
            order_dict=table.get("order_by")
            groupByList=[]
            for obj in column_dict:
                column_value=obj.get("column")
                value = obj.get("column_name")
                fun=obj.get("column_fun")
                if fun and column_value:
                    if fun == "sum":
                        flag=True
                        tempDict={}
                        tempDict["groupColumn"]=value
                        groupByList.append(tempDict)
                        select_stmt += "sum({}),{}, ".format(column_value, "".join(value))
                    elif fun == "avg":
                        flag=True
                        tempDict={}
                        tempDict["groupColumn"]=value
                        groupByList.append(tempDict)
                        select_stmt += "avg({}),{}, ".format(column_value, "".join(value))
                    elif fun == "max":
                        flag=True
                        tempDict={}
                        tempDict["groupColumn"]=value
                        groupByList.append(tempDict)
                        select_stmt += "max({}),{}, ".format(column_value, "".join(value))
                    elif fun == "min":
                        flag=True
                        tempDict={}
                        tempDict["groupColumn"]=value
                        groupByList.append(tempDict)
                        select_stmt += "min({}),{}, ".format(column_value, "".join(value))
                    elif fun == "count":
                        flag=True
                        tempDict={}
                        tempDict["groupColumn"]=value
                        groupByList.append(tempDict)
                        select_stmt += "count({}),{}, ".format(column_value, "".join(value))
                elif fun:
                    if fun == "sum":
                        flag=True
                        tempDict={}
                        tempDict["groupColumn"]=value
                        groupByList.append(tempDict)
                        select_stmt += "sum({}), ".format("".join(value))
                    elif fun == "avg":
                        flag=True
                        tempDict={}
                        tempDict["groupColumn"]=value
                        groupByList.append(tempDict)
                        select_stmt += "avg({}), ".format("".join(value))
                    elif fun == "max":
                        flag=True
                        tempDict={}
                        tempDict["groupColumn"]=value
                        groupByList.append(tempDict)
                        select_stmt += "max({}), ".format("".join(value))
                    elif fun == "min":
                        flag=True
                        tempDict={}
                        tempDict["groupColumn"]=value
                        groupByList.append(tempDict)
                        select_stmt += "min({}), ".format("".join(value))
                    elif fun == "count":
                        flag=True
                        tempDict={}
                        tempDict["groupColumn"]=value
                        groupByList.append(tempDict)
                        select_stmt += "count({}), ".format("".join(value))
                else:
                    tempDict={}
                    tempDict["groupColumn"]=value
                    groupByList.append(tempDict)
                    select_stmt += "{}, ".format("".join(value))
            select_stmt = select_stmt.rstrip(", ") # remove trai
            if len(tableList)==1:
                select_stmt += " FROM {}".format("".join(table_name))
            else: 
                commonColumnsCheck=check_common_columns(tableList,select_stmt)
                if commonColumnsCheck==0:
                    return jsonify("Report can not be made as there are no common columns!!")
                else:
                    tabletemp=[]
                    for table in tableList:
                        tabletemp.append(table.get("name"))
                    select_stmt += " FROM "
                    select_stmt += "{}, ".format("".join(tabletemp,select_stmt))
                    select_stmt+=commonColumnsCheck
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
                        select_stmt+=curd.condition(inputColumn,values,low,high,value,logicalOpe,operator,result)
                    elif commonColumnsCheck!=0:
                        select_stmt+=curd.specialCondition(inputColumn,values,low,high,value,logicalOpe,operator,result)
                        commonColumnsCheck=0
                    else:
                        select_stmt+=curd.condition(inputColumn,values,low,high,value,logicalOpe,operator,result)
            listOfGroup=[]
            listofOrder=[]
            if flag==True:
                for obj in groupByList:
                    value = obj.get("groupColumn")
                    listOfGroup.append(value)
                select_stmt+=" GROUP BY "
                select_stmt += ", ".join(listOfGroup) 
                flag=0
            # yaha having clause ayega....
            if order_dict:
                select_stmt+=" ORDER BY "
                for obj in order_dict:
                    tempDict={}
                    column=obj.get("column")
                    order=obj.get("order")
                    if order=="asc":
                        select_stmt += "{}, ".format("".join(column))
                    elif order=="desc":
                        select_stmt += "{} DESC, ".format("".join(column))
            select_stmt = select_stmt.rstrip(", ") # remove trailing comma
            result2=curd.dbTransactionSelect(select_stmt)
            print(select_stmt)
            ans.append(result2)
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
        return jsonify(ans)
# new code --------------------------------------------------
        
    for table in selected_tables:
        table_name = table.get("name")
        if table_name:
            select_stmt = "SELECT "
            column_dict = table.get("columnNames") # fetch column_dict based on selected table
            order_dict=table.get("order_by")
            groupByList=[]
            for obj in column_dict:
                column_value=obj.get("column")
                value = obj.get("column_name")
                fun=obj.get("column_fun")
                if fun and column_value:
                    if fun == "sum":
                        flag=True
                        tempDict={}
                        tempDict["groupColumn"]=value
                        groupByList.append(tempDict)
                        select_stmt += "sum({}),{}, ".format(column_value, "".join(value))
                    elif fun == "avg":
                        flag=True
                        tempDict={}
                        tempDict["groupColumn"]=value
                        groupByList.append(tempDict)
                        select_stmt += "avg({}),{}, ".format(column_value, "".join(value))
                    elif fun == "max":
                        flag=True
                        tempDict={}
                        tempDict["groupColumn"]=value
                        groupByList.append(tempDict)
                        select_stmt += "max({}),{}, ".format(column_value, "".join(value))
                    elif fun == "min":
                        flag=True
                        tempDict={}
                        tempDict["groupColumn"]=value
                        groupByList.append(tempDict)
                        select_stmt += "min({}),{}, ".format(column_value, "".join(value))
                    elif fun == "count":
                        flag=True
                        tempDict={}
                        tempDict["groupColumn"]=value
                        groupByList.append(tempDict)
                        select_stmt += "count({}),{}, ".format(column_value, "".join(value))
                elif fun:
                    if fun == "sum":
                        flag=True
                        tempDict={}
                        tempDict["groupColumn"]=value
                        groupByList.append(tempDict)
                        select_stmt += "sum({}), ".format("".join(value))
                    elif fun == "avg":
                        flag=True
                        tempDict={}
                        tempDict["groupColumn"]=value
                        groupByList.append(tempDict)
                        select_stmt += "avg({}), ".format("".join(value))
                    elif fun == "max":
                        flag=True
                        tempDict={}
                        tempDict["groupColumn"]=value
                        groupByList.append(tempDict)
                        select_stmt += "max({}), ".format("".join(value))
                    elif fun == "min":
                        flag=True
                        tempDict={}
                        tempDict["groupColumn"]=value
                        groupByList.append(tempDict)
                        select_stmt += "min({}), ".format("".join(value))
                    elif fun == "count":
                        flag=True
                        tempDict={}
                        tempDict["groupColumn"]=value
                        groupByList.append(tempDict)
                        select_stmt += "count({}), ".format("".join(value))
                else:
                    tempDict={}
                    tempDict["groupColumn"]=value
                    groupByList.append(tempDict)
                    select_stmt += "{}, ".format("".join(value))
            select_stmt = select_stmt.rstrip(", ") # remove trailing comma
                # listOfColumns.append(value)
            # column_names = listOfColumns
            # select_stmt += ", ".join(column_names)
            select_stmt += " FROM " + table_name
            
            # yaha tak done he
            
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
                        select_stmt+=curd.condition(inputColumn,values,low,high,value,logicalOpe,operator,result)
                    else:
                        select_stmt+=curd.condition(inputColumn,values,low,high,value,logicalOpe,operator,result)
            listOfGroup=[]
            listofOrder=[]
            if flag==True:
                for obj in groupByList:
                    value = obj.get("groupColumn")
                    listOfGroup.append(value)
                select_stmt+=" GROUP BY "
                select_stmt += ", ".join(listOfGroup) 
                flag=0
            # yaha having clause ayega....
            if order_dict:
                select_stmt+=" ORDER BY "
                for obj in order_dict:
                    tempDict={}
                    column=obj.get("column")
                    order=obj.get("order")
                    if order=="asc":
                        select_stmt += "{}, ".format("".join(column))
                    elif order=="desc":
                        select_stmt += "{} DESC, ".format("".join(column))
            select_stmt = select_stmt.rstrip(", ") # remove trailing comma
            result2=curd.dbTransactionSelect(select_stmt)
            print(select_stmt)
            ans.append(result2)
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

@app.route('/api/getData',methods=["POST"])#this is only for single table selection
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

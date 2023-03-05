import psycopg2
from flask import Flask,jsonify,render_template, session, abort, redirect, request
import requests
connection = psycopg2.connect(user="postgres",password="root",host="localhost",port="5433",database="postgres")
cursor = connection.cursor()

def dataOfTable(tableName,column_dict):
    result=[]
    var=str(tableName)
# Build the SELECT statement based on the dictionary values
    select_stmt = "SELECT "
    column_names = [key for key in column_dict.keys() if column_dict[key]]
    select_stmt += ", ".join(column_names)
    select_stmt += " FROM " + var

    # Execute the SELECT statement and fetch the results
    cursor.execute(select_stmt)
    results = cursor.fetchall()
    for row in results:
        result.append(row)
    return result

def viewData(tableName,column_dict,groupBy,sum1,sum2):
    result=[]
    var=str(tableName)
# Build the SELECT statement based on the dictionary values
# Execute the query and print the results
    
    group_by_column =str(groupBy)
    var1=str(sum1)
# Build the GROUP BY query
    if group_by_column in ['customer_name','price', 'region', 'date']:
        query = ("SELECT {group_by_column}, SUM({var1}) FROM {var} GROUP BY {group_by_column}".format(group_by_column=group_by_column,var1=var1,var=var))
    else:
        print("Invalid column name.")
        query = ""

# Execute the query and print the results
    if query:
        cursor.execute(query)
        results = cursor.fetchall()
        for row in results:
            result.append(row)
    return result
    
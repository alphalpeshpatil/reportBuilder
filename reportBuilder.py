import psycopg2
from flask import Flask,jsonify,render_template, session, abort, redirect, request
import requests
connection = psycopg2.connect(user="postgres",password="root",host="localhost",port="5433",database="postgres")
cursor = connection.cursor()

def viewData(tableName,column_dict,groupBy,sum1,date,start_date,end_date):
    result=[]
    var=str(tableName)
# Build the SELECT statement based on the dictionary values
# Execute the query and print the results
    group_by_column =str(groupBy)
    var1=str(sum1)
# betwenn any date query
    if start_date or end_date:
        query = "SELECT "
        column_names = [key for key in column_dict.keys() if column_dict[key]]
        query += ", ".join(column_names)
        query += " FROM " + var
        query+=" WHERE " + column_dict['sale_date']
        query+=" BETWEEN  " + start_date 
        query+=" AND " + end_date
        # Build the GROUP BY query
    elif group_by_column in ['quantity','customer_name','price', 'region', 'date']:
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
    
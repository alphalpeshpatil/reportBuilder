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
    
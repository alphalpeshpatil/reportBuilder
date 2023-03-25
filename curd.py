import psycopg2

def condition(inputColumn,values,low,high,value,logicalOpe,operator,result):
    select_stmt=""
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
    return str(select_stmt)

def getDataType(tableName,columnName):
    connection = psycopg2.connect(user="postgres",password="root",host="localhost",port="5433",database="postgres")
    try:
        select_cursor = connection.cursor()
        query="SELECT data_type FROM information_schema.columns WHERE table_name = '{}' AND column_name = '{}'".format(tableName,columnName)
        print(query)
        select_cursor.execute(query)
        query_result=select_cursor.fetchall()
        connection.commit()
        if len(query_result)>0:
            columns = select_cursor.description 
            result = [{columns[index][0]:column for index, column in enumerate(value)} for value in query_result]
            return result
        if len(query_result)==0:
            return "No data Found"
    except Exception as ex:
        return str(ex)
    finally:
        select_cursor.close()
        connection.close()

def dbTransactionSelect(query):
    connection = psycopg2.connect(user="postgres",password="root",host="localhost",port="5433",database="postgres")
    try:
        select_cursor = connection.cursor()
        select_cursor.execute(query)
        query_result=select_cursor.fetchall()
        connection.commit()
        if len(query_result)>0:
            columns = select_cursor.description 
            result = [{columns[index][0]:column for index, column in enumerate(value)} for value in query_result]
            return result
        if len(query_result)==0:
            return "No data Found"
    except Exception as ex:
        return str(ex)
    finally:
        select_cursor.close()
        connection.close()

def dbTransactionIUD(query):
    connection = psycopg2.connect(user="postgres",password="root",host="localhost",port="5433",database="postgres")
    try:
        iud_cursor = connection.cursor()
        iud_cursor.execute(query)
        connection.commit()
        return "Success"
    except Exception as ex:
        return str(ex)
    finally:
        iud_cursor.close()
        connection.close()


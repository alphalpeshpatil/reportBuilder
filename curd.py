import psycopg2

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


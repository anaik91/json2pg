import sys
import json 
import psycopg2
from psycopg2.extras import Json, DictCursor

def createconnection(database,host,user,password,port):
    con = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
    print("Database connection opened successfully")
    cur = con.cursor()
    return con,cur

def runquery(con,cur,query):
    cur.execute(query)
    con.commit()

def createTable(con,cur,table,columns):
    query1 = [ '{} TEXT PRIMARY KEY'.format(i) if i == 'id' else '{} TEXT'.format(i) for i in columns ]
    finalquery='CREATE TABLE IF NOT EXISTS {} ( '.format(table) + ','.join(query1) + ')'
    print('Running Query: {}'.format(finalquery))
    try:
        cur.execute(finalquery)
        con.commit()
    except psycopg2.errors.DuplicateTable:
        print('Issue creating Table : {}'.format(table))
        sys.exit(1)

def main():
    table='json2pg'
    f='test.json'
    data=open(f).read()
    data=json.loads(data)
    conn = psycopg2.connect(database="postgres", user='postgres', password='password', host='127.0.0.1', port= '5432')
    cursor = conn.cursor()
    columns = list(data.keys())
    createTable(conn,cursor,table,columns)
    basequery='INSERT INTO {} '.format(table) 
    columnquery = ','.join(columns)
    valuequery = []
    for _,v in data.items():
        if isinstance(v,dict):
            valuequery.append('\'{}\''.format(json.dumps(v)))
        else:
            valuequery.append('\'{}\''.format(v))
    #print(valuequery)
    finalquery = '''{} ({}) VALUES ({})'''.format(basequery,columnquery,','.join(valuequery))
    #print(finalquery)
    try:
        cursor.execute(finalquery)
        conn.commit()
    except psycopg2.errors.UniqueViolation:
        print('Entry {} already Exists'.format(valuequery[0]))
    conn.close()
    print("Database connection closed successfully")


if __name__ == '__main__':
    main()
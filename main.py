import sys,os
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

def createTable(con,cur,table,data):
    #columns = list(data.keys())
    #query1 = [ '{} TEXT PRIMARY KEY'.format(i) if i == 'id' else '{} TEXT'.format(i) for i in columns ]
    query1 = []
    for k,v in data.items():
        if k == 'id':
            query1.append('{} TEXT PRIMARY KEY'.format(k))
        elif isinstance(v,dict) or isinstance(v,list):
            query1.append('{} JSONB'.format(k))
        else:
            query1.append('{} TEXT'.format(k))
    finalquery='CREATE TABLE IF NOT EXISTS {} ( '.format(table) + ','.join(query1) + ')'
    print('Running Query: {}'.format(finalquery))
    try:
        cur.execute(finalquery)
        con.commit()
    except psycopg2.errors.DuplicateTable:
        print('Issue creating Table : {}'.format(table))
        sys.exit(1)

def readjson(filename):
    data=open(filename).read()
    data=json.loads(data)
    return data

def insertdata(conn,cursor,table,data):
    basequery='INSERT INTO {} '.format(table) 
    columnquery = ','.join(list(data.keys()))
    valuequery = []
    for _,v in data.items():
        if isinstance(v,dict) or isinstance(v,list):
            valuequery.append('\'{}\''.format(json.dumps(v)))
        else:
            valuequery.append('\'{}\''.format(v))
    finalquery = '''{} ({}) VALUES ({})'''.format(basequery,columnquery,','.join(valuequery))
    try:
        cursor.execute(finalquery)
        conn.commit()
    except psycopg2.errors.UniqueViolation:
        print('Entry {} already Exists'.format(valuequery[0]))

def main():
    table='json2pg'
    dir = 'files'
    all_files = os.listdir(dir)
    data=readjson('{}/{}'.format(dir,all_files[0]))
    pg_host = os.getenv('pg_host','127.0.0.1')
    pg_db = os.getenv('pg_db','postgres')
    pg_user = os.getenv('pg_user','postgres')
    pg_password = os.getenv('pg_password','password')
    pg_port = os.getenv('pg_port','5432')
    conn,cursor = createconnection(pg_db,pg_host,pg_user,pg_password,pg_port)
    #columns = ['uuid','config','request']
    createTable(conn,cursor,table,data)
    conn.close()
    #sys.exit()
    for eachfile in all_files:
        data=readjson('{}/{}'.format(dir,eachfile))
        conn,cursor = createconnection(pg_db,pg_host,pg_user,pg_password,pg_port)
        insertdata(conn,cursor,table,data)
        conn.close()
    conn.close()
    print("Database connection closed successfully")


if __name__ == '__main__':
    main()
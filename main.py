import sys,os
import json 
import psycopg2

def createconnection(database,host,user,password,port):
    con = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
    print("Database connection opened successfully")
    cur = con.cursor()
    return con,cur

def runquery(con,cur,query):
    cur.execute(query)
    con.commit()

def createTable(con,cur,table,columns):
    query1 = []
    for k in columns:
        if k == 'uuid':
            query1.append('{} TEXT PRIMARY KEY'.format(k))
        elif k == 'config':
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

def insertdata(conn,cursor,table,columns,data):
    basequery='INSERT INTO {} '.format(table) 
    columnquery = ','.join(list(columns))
    valuequery = []
    for k in columns:
        if k == 'uuid':
            valuequery.append('\'{}\''.format(data['id']))
        elif k == 'request':
            valuequery.append('\'{}\''.format('request'))
        else:
            valuequery.append('\'{}\''.format(json.dumps(data)))
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
    columns = ['uuid','config','request']
    createTable(conn,cursor,table,columns)
    conn.close()
    for eachfile in all_files:
        data=readjson('{}/{}'.format(dir,eachfile))
        conn,cursor = createconnection(pg_db,pg_host,pg_user,pg_password,pg_port)
        insertdata(conn,cursor,table,columns,data)
        conn.close()
    conn.close()
    print("Database connection closed successfully")


if __name__ == '__main__':
    main()
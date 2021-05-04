#!/usr/bin/env python
# coding: utf-8

# In[4]:


import re
from dateutil.parser import parse
import pandas as pd                                                        #You may need to pip install these libraries
from sqlalchemy import create_engine
import sqlparse
import time
import payasyougo as pg
import requests
import sys
import os
import json

global Connector_name
Connector_name = 'SendGrid'

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

data = []
gs = None
dt = None
lookUp = None
engine = create_engine('sqlite://')
engines = [['PostgreSQL','postgresql://scott:tiger@localhost/mydatabase'],['Sql Server','mssql+pyodbc://scott:tiger@mydsn'],['Oracle','oracle://scott:tiger@127.0.0.1:1521/sidname'],['MySQL','mysql://scott:tiger@localhost/foo'],['SQLite','sqlite:///foo.db']]
supportedEngines = pd.DataFrame(columns=['Engine Name','Example Connection'],data=engines,index=None)

def initializeAPI(api_key):
    StartTime = time.time()

    global token
    token= api_key
    EndTime = time.time()
    pg.measure_execution_time(LicenseKey,initializeAPI.__name__,StartTime,EndTime,Connector_name)
    return token

def req(url): #Initialize Api
        payload = "{}"
        header = {'Authorization': f'Bearer {token}'}
        req = requests.request("GET", url, headers=header)

        if not req:
            return {}
        else: 
            return req.json()


def fetchDataFromAPI(table, query):
    StartTime = time.time()
    apikeyurl='https://api.sendgrid.com/v3/api_keys'
    access_attempts_url= 'https://api.sendgrid.com/v3/access_settings/activity?limit=20'  
    api_key_scopes= 'https://api.sendgrid.com/v3/scopes'
    enforced_tls = 'https://api.sendgrid.com/v3/user/settings/enforced_tls'
    whitelist_access= 'https://api.sendgrid.com/v3/access_settings/whitelist'
    mail_settings ='https://api.sendgrid.com/v3/mail_settings'
    partner_s_setting= 'https://api.sendgrid.com/v3/partner_settings'
    design= 'https://api.sendgrid.com/v3/designs?page_size=100&summary=true'
    access_requests= 'https://api.sendgrid.com/v3/scopes/requests?limit=50&offset=0'
    alerts= 'https://api.sendgrid.com/v3/alerts'
    user_profile= 'https://api.sendgrid.com/v3/user/profile'
    subuser_profile= 'https://api.sendgrid.com/v3/subusers'
    sender_verification= 'https://api.sendgrid.com/v3/verified_senders/domains'
    
    if table == 'apikey':
        api_keys=req(apikeyurl)
        return dataToTable(api_keys,table,query )
    elif table=='access_attempt':
        access_attempt= req(access_attempts_url)
        return dataToTable(access_attempt,table,query)
    elif table=='key_scopes':
        scopes= req(api_key_scopes)
        return dataToTable(scopes,table,query)
    elif table=='enforced_tls':
        a= req(enforced_tls)
        return dataToTable(a,table,query)
    elif table=='whitelist_access':
        a= req(whitelist_access)
        return dataToTable(a,table,query)
    elif table=='mail_settings':
        a= req(mail_settings)
        return dataToTable(a,table,query)
    elif table=='partner_mail_settings':
        a= req(partner_s_setting)
        return dataToTable(a,table,query)
    elif table=='design':
        a= req(design)
        return dataToTable(a,table,query)
    elif table=='access_requests':
        a= req(access_requests)
        return dataToTable(a,table,query)
    elif table=='alerts':
        a= req(alerts)
        return dataToTable(a,table,query)
    elif table=='user_profile':
        a= req(user_profile)
        return dataToTable(a,table,query)
    elif table=='subuser_profile':
        a= req(subuser_profile)
        return dataToTable(a,table,query)
    elif table=='sender_verification':
        a= req(sender_verification)
        return dataToTable(a,table,query)
    
    else:
        pass
    EndTime = time.time()
    pg.measure_execution_time(LicenseKey,fetchDataFromAPI.__name__,StartTime,EndTime,Connector_name)


def parse(jsn):
    tble={}
    simple={}
    if type(jsn) == list:
        for i in jsn:
            for k, v in i.items():
                if v== 'true':
                    v= True
                elif v== 'false':
                    v = False
                else: 
                    pass
                if type(v) == list:
                    tble[k]= v
                if type(v) == dict:
                    tble[k]= v
                elif type(v) == str or type(v)== int or type(v)== bool:
                    simple[k]=v
            tble['main']= simple
            return tble
    elif type(jsn) == dict:
        for i in jsn.keys():
            print(i)
            for k, v in jsn.items():
                if v== 'true':
                    v= True
                elif v== 'false':
                    v = False
                else: pass
                if type(v) == list:
                    tble[k]= v
                if type(v) == list:
                    tble[k]= v
                elif type(v) == str or type(v)== int or type(v)== bool:
                    simple[k]=v
            tble['main']= simple
            return tble


def querymaker(query):
    columnsPart=[] # where clause value
    select=[]
    subselect=[] # sub query select
    subcol=[]  # sub query col
    col=[]
    table=[]
    updated=[]
    limit=[]
    new=[]
    offset=[]
    subtable=[] # sub query table
    parsed = sqlparse.parse(query)
    stmt=parsed[0]
    where=str(stmt.tokens[-1])

    if query.__contains__("Describe"):
        t =query.split("Describe ")
        return sysQueries(".", t[1])

    if re.search('select', where, re.IGNORECASE):# for sub query
        columnsPart = where.split('in ')[1]
        columnsPart=(columnsPart.replace('(',''))
        columnsPart=(columnsPart.replace(')',''))      
        parse=sqlparse.parse(columnsPart)
        stmts=parse[0]
        subselect=str(stmts.tokens[0])
        subcol=str(stmts.tokens[2])
        subtable=str(stmts.tokens[6])
        select=str(stmt.tokens[0])
        col=str(stmt.tokens[2])
        table=str(stmt.tokens[6])

        if query.lower().__contains__("sys."):
            return sysQueries(table, col)
            
        else:
            jsonres = fetchDataFromAPI(table, query)
        list_df = json_to_df(jsonres,table)
        for df_name,df in list_df.items():
            dataToTable(df, df_name)
        r = engine.execute(query)
        tble = pd.DataFrame(r, index=None, columns=r.keys())
        return tble

    elif (re.search('=', where)):     # for query which has where clause 
        select=str(stmt.tokens[0])
        col=str(stmt.tokens[2])
        table=str(stmt.tokens[6])
        where=str(stmt.tokens[8])
        where=where.split(" ")
        for i in where:
            if i=='=':
                where.remove(i)
            elif i=='or':
                where.remove(i)
            elif i=='and':
                where.remove(i)

        where.pop(0)
        for j in where:
            if(re.search('=',j, re.IGNORECASE)):
                t=j
                where.remove(t)
                j = j.replace('=',' ')
                where.append(j)

        if query.lower().__contains__("sys."):
            return sysQueries(table, col)
            
        else:
            jsonres = fetchDataFromAPI(table, col)
        list_df = json_to_df(jsonres,table)
        for df_name,df in list_df.items():
            dataToTable(df, df_name)
        r = engine.execute(query)
        tble = pd.DataFrame(r, index=None, columns=r.keys())
        return tble

    else:
        parsed = sqlparse.parse(query)
        stmt=parsed[0]
        select=str(stmt.tokens[0])
        col=str(stmt.tokens[2])
        table=str(stmt.tokens[6])
        jsonres = None

        if query.lower().__contains__("sys."):
            return sysQueries(table, col)
            
        else:
            jsonres = fetchDataFromAPI(table, query)
        list_df = json_to_df(jsonres,table)
        for df_name,df in list_df.items():
            dataToTable(df, df_name)
        r = engine.execute(query)
        tble = pd.DataFrame(r, index=None, columns=r.keys())
        return tble
        

def json_to_df(jsn,main_table_name):
    simple = dict()
    dataFrames = dict()
    for key , val in jsn.items():
        if type(val) == list:
            dataFrames[key] = pd.DataFrame(val)
        elif type(val) == dict:
            df = pd.DataFrame.from_dict(val, orient='index', dtype=None)
            dataFrames[key] = df.transpose()
        else:
            simple[key] = val
        #df = pd.DataFrame.from_dict(simple, orient='index', dtype=None)
        #dataFrames[main_table_name] = df.transpose()
    return dataFrames


def sysQueries(query, table):
    qury = query.split(".")
    command = qury[1]
    jsn = pd.read_json("schema.json", orient="records", typ="records")


    if command == "tables":
        for i in table:
            if i == "*":
                tablenames = jsn['Tables'].keys()
                tables = []
                tablestype = []
                for i in tablenames:
                    tables.append(i)
                    tablestype.append(jsn['Tables'][i]['datatype'])
                df = pd.DataFrame(tables, columns=["tablename"])
                df["datatype"] = tablestype
                return df
            else:
                col = jsn['Tables'][i]['columns']
                colinfo = pd.DataFrame(col)
                colinfo['tablename'] = i
                return colinfo

    elif command == "constraints":
        for i in table:
            tables = []
            fk = []
            pk = []
            colname = []
            if i == "*":
                tablenames = jsn['Tables'].keys()
                for i in tablenames:
                    if not i.__contains__("sys."):
                        cols = jsn['Tables'][i]['columns']
                        for col in cols:
                            if len(col['constraint']) > 0:
                                tables.append(i)
                                colname.append(col['column'])
                                if col['constraint'].__contains__("PRIMARY"):
                                    fk.append("NULL")
                                    pk.append(col['constraint'])
                                else :
                                    fk.append(col['constraint'])
                                    pk.append("NULL")
                df = pd.DataFrame(tables, columns=["tablename"])
                df["PK"] = pk
                df["FK"] = fk
                df["column name "] = colname
                return df
            else:
                col = jsn['Tables'][i]['columns']
                for j in col:
                    if len(j['constraint']) > 0:
                        tables.append(i)
                        colname.append(j['column'])
                        if j['constraint'].__contains__("PRIMARY KEY"):
                            fk.append("NULL")
                            pk.append(j['constraint'])
                        else :
                            fk.append(j['constraint'])
                            pk.append("NULL")
                        df = pd.DataFrame(tables, columns=["tablename"])
                        df["PK"] = pk
                        df["FK"] = fk
                        df["column name "] = colname
                        return df    
                        
    elif command == "methods":
        for i in table:
            data = []
            if i == "*":
                methodsname = jsn['Methods'].keys()
                for i in methodsname:
                    dic = dict()
                    dic['methodname'] = i
                    dic["return"] = jsn['Methods'][i]['return']
                    dic["returntype"] = jsn['Methods'][i]['returntype']
                    dic["description"] = jsn['Methods'][i]['description']
                    dic["parameters"] =jsn['Methods'][i]['parameters']
                    data.append(dic)
                df = pd.DataFrame(data)
                return df
            else:
                data = jsn['Methods'][i]['parameters']
                parameters = pd.DataFrame(data)
                parameters['tablename'] = i
                return parameters

    elif command == "logs":
        col = ["Logs"]
        return pd.DataFrame(pg.search(LicenseKey, command), columns=col)

    elif command == "delta":
        jsn = pd.read_json("schema.json", orient="records", typ="records")
        for i in table:
            tables = []
            deltafield = []
            if i == "*":
                tablenames = jsn['Tables'].keys()
                for i in tablenames:
                    if not i.__contains__("sys."):
                        cols = jsn['Tables'][i]['columns']
                        for col in cols:
                            if len(col['constraint']) > 0:
                                if col['constraint'].__contains__("PRIMARY"):
                                    tables.append(i)
                                    deltafield.append(col['column'])
                df = pd.DataFrame(tables, columns=["tablename"])
                df["deltafield"] = deltafield
                return df

    elif command == "connectionstring":
        for i in table:
            data = []
            if i == "*":
                methodsname = jsn['Methods'].keys()
                for i in methodsname:
                    dic = dict()
                    dic["target"] = i
                    dic["connectinparameters"] =  jsn['Methods'][i]['parameters']
                    data.append(dic)
                df = pd.DataFrame(data)
                return df
            else:
                data = jsn['Methods'][i]['parameters']
                parameters = pd.DataFrame(data)
                parameters['target'] = i
                return parameters

    elif command == "version":
        return print("version")

    elif command == "usage":
        #[licensekey	remaining_time	total_allowed_time	total_time_spent	start_time	end_time	total_time_taken	rows_allowed	rows_fetched	month	machine_name	mac	ip)
        col = ["Total Time Spent ", "Remaining Time", " Total Time alloted"]
        return pd.DataFrame(pg.search(LicenseKey, command), columns=col)


    elif command == "license":     
        col = ["LicenseKey", "ActivateDate", "ExpireDate", "Total Time Spent", "Remaining Time"]   
        return pd.DataFrame(pg.search(LicenseKey, command), columns=col,index=None)
    
    else:
        tables = jsn['Tables'][table]
        data = []
        constraints = []
        pk = ""
        fk = ""
        Statement = 'CREATE TABLE '+ table+'(' 
        for i in tables['columns']:
            Statement = Statement+i['column']+" "+i['datatype']+",\n"
            if i['constraint'].__contains__("PRIMARY"):
                pk = "PRIMARY KEY ("+i['column']+"),\n"
            elif i['constraint'].__contains__("FOREIGN "):
                fk = i['constraint']
            else:
                pass
        describedtable = Statement[:-2]+",\n"+pk+fk+');'

        return describedtable


def dataToTable(results,table,query):
    StartTime = time.time()
#     print(results)
    if table== 'apikey' or table== 'access_attempt' or table== 'whitelist_access' or table== 'mail_settings' or    table=='partner_mail_settings' or table== 'design' or table== 'access_requests' : 
        if results['result']:
            t=pd.DataFrame(results['result'])
        else:
            
            print('no data in table')
            sys.exit(0)
#             os._exit(0)
    elif table=='sender_verification':
        a=schema_parse(results['results'])
        t=pd.DataFrame(a, index=[0])
    elif table=='subuser_profile':
        t=pd.DataFrame(results)
    elif table == 'key_scopes':
        t=pd.DataFrame(results['scopes'])
   
    elif table == 'enforced_tls':
        t=pd.DataFrame(results, index=[0])
    elif table == 'alerts':
        t=pd.DataFrame(results, index=[0])
    elif table == 'user_profile':
        t=pd.DataFrame(results, index=[0])
    else:
        pass
#     print(results)
    t.to_sql(table, con=engine, if_exists='replace') 
    r = engine.execute(query)
    tble = pd.DataFrame(r, index=None,columns=r.keys())
    print(tble)
    return tble   
    EndTime = time.time()
    pg.measure_execution_time(LicenseKey,dataToTable.__name__,StartTime,EndTime,Connector_name)


def check_data_type(val):

    if type(val) is int:
        datatype = "int"
        return datatype

    elif type(True) == type(val) or type(False) == type(val):
        datatype = "boolean"
        return datatype

    elif val == "":
        pass

    else:
        try:
            datetime.datetime.strptime(val, "%Y-%m-%dT%H:%M:%S")
            datatype = "datetime"
            return datatype

        except :
            datatype = "varchar(255)"
            return datatype


def scheduler_task(data, rows, file_name=""):
    StartTime = time.time()
    
    # assuming.......data's type is pandas framework.......
    global END, START, STOP

    END = START + rows

    if END > len(data):
        END = len(data)
        STOP = 1

    print(data.iloc[START: END])
    START += rows

    if len(file_name) > 1:
        data.to_csv(file_name)
    else:
        print('')
        print(data)
    EndTime = time.time()
    pg.measure_execution_time(LicenseKey,scheduler_task.__name__,StartTime,EndTime,Connector_name)
def schema_parse(c):
    scopes=''
    final={}
    for i in c:
        for k,v in c.items():
            for i in v:
                if scopes:
                    scopes = scopes + ";" +i 
                else:
                    scopes=i        
            final['scopes']= scopes
    #         print(final)
            return final

def schedule_data(sec, rows, df,filename):
    StartTime = time.time()
    print("Total rows : ", len(df))
    schedule.every(sec).seconds.do(
        lambda: scheduler_task(data=df, rows=rows, file_name=filename))  # file_name is optional

    while True:
        schedule.run_pending()
        if STOP:
            schedule.clear()
            break
    EndTime = time.time()
    pg.measure_execution_time(LicenseKey,schedule_data.__name__,StartTime,EndTime,Connector_name)


def connectEngine(username, password, server, database, tableName, df):
    StartTime = time.time()
    alchemyEngine = create_engine(
    f"postgresql+psycopg2://"+username+":"+password+"@"+server+"/"+database+", pool_recycle=3600")
    
    try:
        con = alchemyEngine.connect()
        df.to_sql(
            tableName, con, if_exists='replace')
        con.close()
        print("Data uploaded to table "+tableName)
    except Exception as e:
        print (getattr(e, 'message', repr(e)))
    EndTime = time.time()
    pg.measure_execution_time(LicenseKey,ConnectEngine.__name__,StartTime,EndTime,Connector_name)
    

def Pagination(data, number_of_page=0):
    StartTime = time.time()
    if number_of_page > 0:
        per_page = math.ceil(len(data) / number_of_page)
        total_page = number_of_page
    # else:
    #     total_page = len(data)

    count = 1
    print("Enter page number to show its data. \n")
    while True:
        print(data.iloc[((count - 1) * per_page): (count * per_page)])
        print("Showing page : ", count, " / ", total_page,"\n")
        key = input("Press 'A' or 'D' to navigate or jump to page no: ")
        if (key.isdigit()):
            page = int(key)
            if page > total_page:
                print('Page doesnot exist')
            else:
                count = page
        elif(key.lower() in ['a','d']):
            lower = key.lower()
            if lower == 'a':
                if count > 1:
                    count -= 1
            elif lower == 'd':
                if count < total_page:
                    count += 1
        else:
            print("Invalid page number")
            pass
    EndTime = time.time()
    pg.measure_execution_time(LicenseKey,Pagination.__name__,StartTime,EndTime,Connector_name)


# Done with this code to be understand    



if __name__ == '__main__':
    LicenseKey = "QWERTY-ZXCVB-6W4HD-DQCRG"
    api_key='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'
#     print(req('https://api.sendgrid.com/v3/user/profile'))
    initializeAPI(api_key)
    q= querymaker("select * from sender_verification")
    


# In[ ]:





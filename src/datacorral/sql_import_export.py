
## SQL TO DF
##servername: bbsqldev01
##database: sql database name - crm, dw2
##query = sql query 

import pyodbc
import pandas as pd
import sqlalchemy as sql
import urllib


from sqlalchemy import create_engine
import urllib
import pyodbc 

class sql_to_df():
    def __init__(self,servername, database, query):
        self.servername=servername
        self.database=database
        self.query=query
    def run(self): 
            ##create connection
        connection_string = "Driver={SQL Server Native Client 11.0};"+"Server={0};Database={1};Trusted_connection=yes;".format(self.servername,self.database)
        quoted = urllib.parse.quote_plus(connection_string)
            ##connecting to the server
        engine=create_engine(f'mssql+pyodbc:///?odbc_connect={quoted}')
            #taking the column names of table on sql
        query= self.query
        df = pd.read_sql_query(query, engine)
        print(df.info())
        return df

# query=("SELECT TOP 10 * FROM STG_OMNILINES")
# df=sql_to_df('bbsqldev01','crm', query).run()


### DF TO SQL 
##servername: bbsqldev01
##database: sql database name - crm, dw2
##df: dataframe in python 
##method: ‘fail’, ‘replace’, ‘append’
##myTable: name of table in SQL database
import pandas as pd
from sqlalchemy import create_engine
import urllib
import pyodbc 

class df_to_sql():
    def __init__(self,servername, database, df, method, myTable):
        self.servername=servername
        self.database=database
        self.df=df
        self.myTable=myTable
        self.method=method
    def run(self): 
            ##create connection
        connection_string = "Driver={SQL Server Native Client 11.0};"+"Server={0};Database={1};Trusted_connection=yes;".format(self.servername,self.database)
        quoted = urllib.parse.quote_plus(connection_string)
            ##connecting to the server
        engine=create_engine(f'mssql+pyodbc:///?odbc_connect={quoted}')
            #taking the column names of table on sql
        query= "SELECT TOP 1 * FROM {0}".format(self.myTable)
        sql_table = pd.read_sql_query(query, engine)
            
             #convert column name to list and make them all lowercase
        a=(map(lambda x: x.lower(), sql_table.columns))
        b=(map(lambda x: x.lower(), self.df.columns))
        
        print(list(a), list(b))
            #if column name in table on sql is the same as df column name, then export data. Else, error message
        if sorted(list(a))==sorted(list(b)):
            try:
                with engine.connect() as cnn:
                    self.df.to_sql(self.myTable,con=cnn, if_exists=self.method, index=False)
                    print(self.myTable+' loaded successfully')
                   
            except Exception as e :
                e=str(e).replace(".","")
                print(f"{e} in Database." )
        else:
            print("header name doesn't match", list(a))

# df_to_sql(servername='BBSQLDEV01',database='crm', df=df2, method='append', myTable='testCustomerGeocode').run()
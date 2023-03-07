from msilib.schema import Directory
import os
from time import time
import pandas as pd
import calendar, time;
#declare filepath and interval variables
from pyspark.sql import SparkSession
import cx_Oracle
import getpass
#import oracledb
#import mysql.connector
import pyodbc
import re
import numpy

#oracledb.init_oracle_client()
#read from sql files and store in string
file1 = open('comparison.sql', 'r')
sql_file = file1.readlines()
  
#count = 0
# Strips the newline character
#for line in sql_file:
#    count += 1
#    sql_str = "Line{}: {}".format(count, line.strip())
sql_str = sql_file[0]
    
# Creating connection object for SQL Server 
cnxn_str = ("Driver={SQL Server Native Client 11.0};"
            "Server=ADCSQLD031,2526;"
            "Database=master;"
            "Trusted_Connection=yes;")
cnxn = pyodbc.connect(cnxn_str)

mouse = cnxn.cursor()

mouse.execute('SELECT ConnID, Q_STARTTIME_CDT, Q_QUEUEID, A_QUEUEID, A_AGENTID, Q_STARTTIME, Q_ENDTIME, A_ANSWERTIME, A_RELEASETIME, Q_CONNID, A_CONNID FROM [PDLReportArchive].[dbo].[WMVoiceDump_View] WHERE Q_StartTime_CDT BETWEEN \'2023-01-18 00:00:00\' AND \'2023-01-18 23:59:59\' AND Q_EndTime_CDT BETWEEN \'2023-01-18 00:00:00\' AND \'2023-01-18 23:59:59\' ORDER BY Q_StartTime_CDT;')

print("Successfully connected to SQL Server")


# fetch all the rows in sql server
sql_server_list = mouse.fetchall()


#Creating connection objects for Prod Server 
connection_prod = cx_Oracle.connect(
    user="cegbuchu",
    password="Chuks30870$",
    dsn="cctrptpdb/cctrptp_toad_srv")


print("Successfully connected to Prod Oracle Database")
cursor_prod = connection_prod.cursor()


connection = cx_Oracle.connect(
    user="ODS2_RPT_ADMIN",
    password="k53m21c81e",
    dsn="cctrptddb/cctrptd_ods2_rpt_srv")


print("Successfully connected to Oracle Database")

cursor = connection.cursor()
qry = sql_str
print(type(sql_str))
print(sql_str)
#qry = 'SELECT ConnID, Q_STARTTIME_CDT, Q_QUEUEID, A_QUEUEID, A_AGENTID, Q_STARTTIME, Q_ENDTIME, A_ANSWERTIME, A_RELEASETIME, Q_CONNID, A_CONNID FROM ODS2_RPT_ADMIN.WM_VOICE_DUMP_FINAL WHERE Q_StartTime_CDT BETWEEN TO_DATE(\'01-18-2023 00:00:00\', \'MM-DD-YYYY HH24:MI:SS\') AND TO_DATE(\'01-18-2023 23:59:59\', \'MM-DD-YYYY HH24:MI:SS\') AND Q_EndTime_CDT BETWEEN TO_DATE(\'01-18-2023 00:00:00\', \'MM-DD-YYYY HH24:MI:SS\') AND TO_DATE(\'01-18-2023 23:59:59\', \'MM-DD-YYYY HH24:MI:SS\') \n  ORDER BY Q_StartTime_CDT'
#qry2= 'SELECT ConnID, Q_STARTTIME_CDT, Q_QUEUEID, A_QUEUEID, A_AGENTID, Q_STARTTIME, Q_ENDTIME, A_ANSWERTIME, A_RELEASETIME, Q_CONNID, A_CONNID FROM ODS2_RPT_ADMIN.WM_EMAIL_DUMP_COMPLETED WHERE Q_StartTime_CDT BETWEEN TO_DATE(\'01-18-2023 00:00:00\', \'MM-DD-YYYY HH24:MI:SS\') AND TO_DATE(\'01-18-2023 23:59:59\', \'MM-DD-YYYY HH24:MI:SS\') AND Q_EndTime_CDT BETWEEN TO_DATE(\'01-18-2023 00:00:00\', \'MM-DD-YYYY HH24:MI:SS\') AND TO_DATE(\'01-18-2023 23:59:59\', \'MM-DD-YYYY HH24:MI:SS\') \n  ORDER BY Q_StartTime_CDT'
#qry2 = 'select * from WM_PDL_DUMP'
#print(type(qry))

cursor.execute(qry)
cursor_prod.execute(qry)


# fetch all the rows 
res = cursor.fetchall()
res2 = cursor_prod.fetchall()




    
#open write file
with open('wm_comparisons_pdl.txt', 'w') as outfile1:      

    number_colunms = len(res[0])
    print(number_colunms)
    
    
    list_column = [[] for i in range(number_colunms)] 
    print(list_column)

    #initializing list and dataframe objects
    #Prod list initializing

    
    #queue_legacy_prod = []
    #starttime_legacy_prod= []
    #connID_legacy_prod = []
    #A_Q_oracle_prod=[]
    #Agentid_oracle_prod=[]
    #Q_start_oracle_prod=[]
    #A_answer_oracle_prod = []
    #A_release_oracle_prod =[]
    #Q_end_oracle_prod=[]
    #Q_connid_oracle_prod =[]
    #A_connid_oracle_prod = []
    
    #Oracle Dev list initializing
    queue_legacy = []
    starttime_legacy= []
    connID_legacy = []
    A_Q_oracle=[]
    Agentid_oracle=[]
    Q_start_oracle=[]
    Q_end_oracle=[]
    A_answer_oracle = []
    A_release_oracle =[]
    Q_connid_oracle =[]
    A_connid_oracle = []
    
    
    #Sql Server list Initializing
    queue_pdl = []
    starttime_pdl= []
    connID_pdl = []
    A_Q_sql = []
    Agentid_sql=[]
    Q_start_sql=[]
    Q_end_sql=[]
    A_answer_sql =[]
    A_release_sql = []
    Q_connid_sql = []
    A_connid_sql = []
    
    
    
    #Dataframe Initializing
    data = {}
    data_sql = {}
    data_prod = {}

    #print(res2)
    #Loop through the tables and add them into list which will serve as the columns for the dataframe 
    
    for l in list_column:
        for line in res:
            array = list(line)
            for column in line:
       
        #first_item = array[0]
                #connID_legacy.append(array[0])
                #starttime_legacy.append(array[1])
                #queue_legacy.append(array[2])
                #A_Q_oracle.append(array[3])
                #Agentid_oracle.append(array[4])
                #Q_start_oracle.append(array[5])
                #Q_end_oracle.append(array[6])
                #A_answer_oracle.append(array[7])
                #A_release_oracle.append(array[8])
                #Q_connid_oracle.append(array[9])
                #A_connid_oracle.append(array[10])
                l.append(array[column])
        
        print(l)

                
                count = 0
                for i in list_column:
                count = count+1
                l.append(array[count])
                if  count == 10:
                    break
    
    
            
     

    for lines in sql_server_list:
        #print(line)
        array2 = list(lines)
        #first_item = array[0]
        connID_pdl.append(array2[0])
        queue_pdl.append(array2[2])
        starttime_pdl.append(array2[1])
        A_Q_sql.append(array2[3])
        Agentid_sql.append(array2[4])
        Q_start_sql.append(array2[5])
        Q_end_sql.append(array2[6])
        A_answer_sql.append(array2[7])
        A_release_sql.append(array2[8])
        Q_connid_sql.append(array2[9])
        A_connid_sql.append(array2[10])

    for line in res2:
        array = list(line)
        #first_item = array[0]
        connID_legacy_prod.append(array[0])
        starttime_legacy_prod.append(array[1])
        queue_legacy_prod.append(array[2])
        A_Q_oracle_prod.append(array[3])
        Agentid_oracle_prod.append(array[4])
        Q_start_oracle_prod.append(array[5])
        Q_end_oracle_prod.append(array[6])
        A_answer_oracle_prod.append(array[7])
        A_release_oracle_prod.append(array[8])
        Q_connid_oracle_prod.append(array[9])
        A_connid_oracle_prod.append(array[10])

    
        
    

    #DataFrames created using lists from above 
    data = {
    'starttime': starttime_legacy,
    'queue': queue_legacy,
    'connID': connID_legacy,
    'A_Queue': A_Q_oracle,
    'AGENTID': Agentid_oracle,
    'Q_start': Q_start_oracle,
    'Q_endtime': Q_end_oracle,
    'answertime':  A_answer_oracle,
    'A_releasetime': A_release_oracle,
    'Q_connid': Q_connid_oracle,
    'A_connid': A_connid_oracle,}

    data_sql = {
    'starttime': starttime_pdl,
    'queue': queue_pdl,
    'connID': connID_pdl,
    'A_Queue': A_Q_sql,
    'AGENTID': Agentid_sql,
    'Q_start': Q_start_sql,
    'Q_endtime': Q_end_sql,
    'answertime':  A_answer_sql,
    'A_releasetime': A_release_sql,
    'Q_connid': Q_connid_sql,
    'A_connid': A_connid_sql,
    }
     
    data_prod = {
    'starttime': starttime_legacy_prod,
    'queue': queue_legacy_prod,
    'connID': connID_legacy_prod,
    'A_Queue': A_Q_oracle_prod,
    'AGENTID': Agentid_oracle_prod,
    'Q_start': Q_start_oracle_prod,
    'Q_endtime': Q_end_oracle_prod,
    'answertime':  A_answer_oracle_prod,
    'A_releasetime': A_release_oracle_prod,
    'Q_connid': Q_connid_oracle_prod,
    'A_connid': A_connid_oracle_prod,}


    
    df = pd.DataFrame(data)
    #df_pdl = pd.DataFrame(data2)
    df_prod = pd.DataFrame(data_prod)
    
    
    
    #compared_frames_queue = compared_frames.groupby(['queue']).size().reset_index()
    #compared_frames_queue.columns = ['queue','Count']
    
    #compared_frames = pd.concat([df,df_prod]).drop_duplicates(keep = False)
    #compared_frames= pd.concat([df,df_pdl]).drop_duplicates(subset = ['starttime','answertime','A_releasetime'], keep=False)
    #A_connid,A_answertime, A_Endtime
    #compared_frames = pd.merge(df, df_pdl, how='inner', left_on='queue', right_on='queue')
    #compared_frames = pd.concat([df, df_pdl],axis=1, keys = ['df', 'df_pdl'])
    compared_frames = pd.concat([df, df_prod],axis=1, keys = ['df', 'df_prod'])
    
    #compared_frames = compared_frames[~compared_frames['df']['Q_endtime'].isin(compared_frames['df_prod']['Q_endtime'])]
    #compared_frames = compared_frames[~compared_frames['df']['answertime'].isin(compared_frames['df_prod']['answertime'])]
    #compared_frames = compared_frames[~compared_frames['df']['A_releasetime'].isin(compared_frames['df_prod']['A_releasetime'])]
    #compared_frames = compared_frames[~compared_frames['df']['starttime'].isin(compared_frames['df_prod']['starttime'])]
    compared_frames = compared_frames[~compared_frames['df']['connID'].isin(compared_frames['df_prod']['connID'])]
    #df.where(df.connID!=df_prod.connID).notna()

    
    
    compared_frames = compared_frames[~compared_frames['df']['queue'].isin(compared_frames['df_prod']['queue'])]
    
    #compared_frames = pd.concat([df,df_pdl]).drop_duplicates().reset_index(drop = True)
    
    #df2 = df.groupby(['A_Queue']).size().reset_index()
    #df2.columns = ['A_Queue','connID','A_Queue','AGENTID', 'Q_start','Q_endtime','answertime','A_releasetime','Q_connid','A_connid','Count']
    #print(df2)

    #df3 = df_pdl.groupby(['A_Queue']).size().reset_index()
    #df3.columns = ['A_Queue','connID','A_Queue','AGENTID', 'Q_start','Q_endtime','answertime','A_releasetime','Q_connid','A_connid','Count']
    #print(df3)

   #,'connID','A_Queue','AGENTID', 'Q_start','Q_endtime','answertime','A_releasetime','Q_connid','A_connid','Count'
    #compared_frames = pd.concat([df,df_pdl]).reset_index(drop = False)
    #compared_frames = pd.concat([df2, df3])
    #compared_frames = compared_frames.reset_index(drop=True)
    #compared_frames_group = compared_frames.groupby(list(compared_frames.columns))
    #idx = [x[0] for x in compared_frames_group.groups.values() if len(x) == 1]
    #compared_frames.reindex(idx)
    
    #cf_list = []
    #for index, row in compared_frames.iterrows():
    #    cf_list.append(row['queue'])
    
    
    dfAsString = compared_frames.to_string(header=False, index=False)
    
    
    outfile1.write(dfAsString)


#,'connID','A_Queue','AGENTID', 'Q_start','Q_endtime','answertime','A_releasetime','Q_connid','A_connid'

        
   
        
#close file       
outfile1.close()
             
                    

    


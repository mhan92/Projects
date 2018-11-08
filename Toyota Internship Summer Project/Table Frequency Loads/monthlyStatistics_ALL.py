# -*- coding: utf-8 -*-
"""
Created on Mon Jul  9 14:51:13 2018

@author: hanm1
"""

import os
import xml.etree.ElementTree as et
import pandas as pd
import pyodbc
from datetime import timedelta, date, datetime
import time

start = time.time()

## GOAL: Monthly ran script that calculates average and standard devation record load.

# Assign working directory
os.chdir(r'C:\Users\hanm1\Desktop\TableCheck')
os.getcwd()

#######################
### XML FILE CONFIG ###
#######################

# import the xml configuration file to retrive values
tree = et.parse('config.xml')
root = tree.getroot()

# gets UID, Hostname, Port, and Database that is used when logging into Netezza
uid = root.find('Netezza/uid').text
hostname = root.find('Netezza/hostname').text
port = root.find('Netezza/port').text
database = root.find('DATABASE/db').text

## used for pulling the log table into the script
### the dates from the log table are used to determine whether a table should have recieved records for it's frequency
#### this date has to be the same date used for the TableMonthlyStats.py script
# pull initial start date for analysis
start_date = root.find('DATABASE/startDate').text
# formatting date for sql query
start_date_format = datetime.strptime(start_date, '%Y%m%d').date()

# pull end date for analysis
end_date = root.find('DATABASE/endDate').text
# formatting date for sql query
end_date_format = datetime.strptime(end_date, '%Y%m%d').date()

# Automatically determine the current date for script when it runs 
## for analysis purposes, we will use current day minus 1
curr_date = date.today()
curr_date = (curr_date - timedelta(days=1))

# formats the current date for output purposes
dateformat = curr_date.strftime('%Y%m%d')

# Pulls Mart and log table from the xml file
db = (root.find('DATABASE/db').text)


##########################
### NETEZZA CONNECTION ###
##########################

# Connection settings for ODBC
conn = pyodbc.connect("DRIVER={NetezzaSQL};SERVER="+hostname+";PORT="+port+";DATABASE="+database+
                      ";UID="+uid+";PWD="+root.find('Netezza/pwd').text+";DSN=NZSQL")
cursor = conn.cursor()

#####################
### PROGRAM START ###
#####################

# empty dataframe to store statistics
stat_sum = pd.DataFrame()

# pull table frequency for separate python script (tablefrequency.py)
freqRef = pd.read_csv('table_frequency_{}_ALL.csv'.format(db))

# subset tables that have frequency classifications
freqRef = freqRef.loc[(freqRef['FREQ'] == 'daily') | (freqRef['FREQ'] == 'monthly') | (freqRef['FREQ'] == 'weekly') |
                       (freqRef['FREQ'] == 'quarterly') | (freqRef['FREQ'] == 'NO DATE COL') ]

# loop through multiple log tables within database
for tbl in freqRef['TABLE_NAME']:
    
    headersSQL = 'select * from {}..{} limit 1'.format(db, tbl)
    headers_list = pd.read_sql_query(headersSQL, conn).columns.tolist()
    
    # detects if the current database is hub
    if db == 'HUB':
        
        # in the hub database, we will only look at as_of date
        dateCol = [col for col in headers_list if 'AS_OF' in col]
        dateCol_dt = [col for col in dateCol if '_DATE' in col or '_DT' in col]
        
        
    else:
        # Separate out DM_CREATED, DM_MODIFIED, and DM_LOADED
        dateCol = [col for col in headers_list if 'DM_C' in col]
        
        # Only look at DATE or DT columns instead of BY
        dateCol_dt = [col for col in dateCol if '_DATE' in col or '_DT' in col]
        
        # Use different date columns for tables that don't have dm_created (LOAD or AS_OF)
        if len(dateCol_dt) == 0:
            dateCol = [col for col in headers_list if 'LOAD' in col or 'AS_OF' in col]
            dateCol_dt = [col for col in dateCol if '_DATE' in col or '_DT' in col]


    ###############################
    ### MEAN/STDDEV CALCULATION ###
    ###############################
        
    if freqRef.loc[freqRef['TABLE_NAME'] == tbl]['FREQ'].iloc[0] == 'daily':

        # sql query for the log table
        sql = """select date({4}), count(1) from {0}..{1} 
                 where date({4}) between TO_DATE('{2}', 'yyyymmdd') and TO_DATE('{3}' , 'yyyymmdd')
                 group by date({4})""".format(db, tbl, start_date, end_date, dateCol_dt[0])
        day_df = pd.read_sql_query(sql, conn)
         
        # mean and stddev from log table
        dayAvg = day_df['COUNT'].mean()
        dayStddev = day_df['COUNT'].std()
    
        # appending results to dataframe
        stat_sum = stat_sum.append({'TABLE_NAME': tbl, 'RECORD_COUNT_AVG': dayAvg, 'RECORD_COUNT_STDDEV': dayStddev, 
                                    'DATE_COLLECTED': dateformat, 'FREQ': 'daily'}, ignore_index=True)
    
    elif freqRef.loc[freqRef['TABLE_NAME'] == tbl]['FREQ'].iloc[0] == 'weekly':
        
        # sql query for the log table
        sql = """select date({4}), count(1) from {0}..{1} 
                 where date({4}) between TO_DATE('{2}', 'yyyymmdd') and TO_DATE('{3}' , 'yyyymmdd')
                 group by date({4})""".format(db, tbl, start_date, end_date, dateCol_dt[0])
        week_df = pd.read_sql_query(sql, conn)
        
        # if there are no records for the given
        if len(week_df) == 0:
            stat_sum = stat_sum.append({'TABLE_NAME': tbl, 'RECORD_COUNT_AVG': 'OUTSIDE TIME PERIOD', 'RECORD_COUNT_STDDEV': 'OUTSIDE TIME PERIOD', 
                                        'DATE_COLLECTED': dateformat, 'FREQ': 'weekly'}, ignore_index=True)
        else:
            week_df['DATE'] = pd.to_datetime(week_df['DATE'])
            
            # calculate total records per week
            WeekCount = week_df.groupby(pd.Grouper(key='DATE',freq='W'))['COUNT'].sum().reset_index().sort_values('DATE')
    
            # calculate weekly averages/stddev
            weekAvg = WeekCount['COUNT'].mean()
            weekStddev = WeekCount['COUNT'].std()
           
            # appending results to dataframe
            stat_sum = stat_sum.append({'TABLE_NAME': tbl, 'RECORD_COUNT_AVG': weekAvg, 'RECORD_COUNT_STDDEV': weekStddev, 
                                        'DATE_COLLECTED': dateformat, 'FREQ': 'weekly'}, ignore_index=True)
    
    elif freqRef.loc[freqRef['TABLE_NAME'] == tbl]['FREQ'].iloc[0] == 'monthly':
            
        # sql query for the log table
        sql = """select date({4}), count(1) from {0}..{1} 
                 where date({4}) between TO_DATE('{2}', 'yyyymmdd') and TO_DATE('{3}' , 'yyyymmdd')
                 group by date({4})""".format(db, tbl, start_date, end_date, dateCol_dt[0])
        month_df = pd.read_sql_query(sql, conn)
        
        month_df['DATE'] = pd.to_datetime(month_df['DATE'])
        
        # calculate total records per month 
        MonthCount = month_df.groupby(pd.Grouper(key='DATE', freq='M'))['COUNT'].sum().reset_index().sort_values('DATE')

        # calculate weekly averages/stddev
        monthAvg = MonthCount['COUNT'].mean()
        monthStddev = MonthCount['COUNT'].std()
        
        # appending results to dataframe
        stat_sum = stat_sum.append({'TABLE_NAME': tbl, 'RECORD_COUNT_AVG': monthAvg, 'RECORD_COUNT_STDDEV': monthStddev, 
                                    'DATE_COLLECTED': dateformat, 'FREQ': 'monthly'}, ignore_index=True)
    
    
    elif freqRef.loc[freqRef['TABLE_NAME'] == tbl]['FREQ'].iloc[0] == 'quarterly':
            
        # sql query for the log table
        sql = """select date({4}), count(1) from {0}..{1} 
                 where date({4}) between TO_DATE('{2}', 'yyyymmdd') and TO_DATE('{3}' , 'yyyymmdd')
                 group by date({4})""".format(db, tbl, start_date, end_date, dateCol_dt[0])
        quarter_df = pd.read_sql_query(sql, conn)
        
        quarter_df['DATE'] = pd.to_datetime(quarter_df['DATE'])

        # calculate total quarter frequency with business quarter ending in march (BQ-MAR) then resetting the index and sorting the values by the date
        QuarterCount = quarter_df.groupby(pd.Grouper(key='DATE', freq='BQ-MAR'))['COUNT'].sum().reset_index().sort_values('DATE')

        # calculate weekly averages/stddev
        quarterAvg = QuarterCount['COUNT'].mean()
        quarterStddev = QuarterCount['COUNT'].std()
        
        # appending results to dataframe
        stat_sum = stat_sum.append({'TABLE_NAME': tbl, 'RECORD_COUNT_AVG': quarterAvg, 'RECORD_COUNT_STDDEV': quarterStddev, 
                                    'DATE_COLLECTED': dateformat, 'FREQ': 'quarterly'}, ignore_index=True)
    else:
        sql = '''select count(1) from {0}..{1}'''.format(db, tbl)
        noDateDf = pd.read_sql_query(sql, conn)
        noDateAvg = noDateDf['COUNT'].mean()
        noDateStddev = noDateDf['COUNT'].std()
        
        stat_sum = stat_sum.append({'TABLE_NAME': tbl, 'RECORD_COUNT_AVG': noDateAvg, 'RECORD_COUNT_STDDEV': noDateStddev, 
                                    'DATE_COLLECTED': dateformat, 'FREQ': 'NO DATE COL'}, ignore_index=True)
# save stat_summary to csv
stat_sum.to_csv('table_statistics_{}_ALL.csv'.format(db), index = False)
    
cursor.close()
conn.close()


end = time.time()

print(end-start)
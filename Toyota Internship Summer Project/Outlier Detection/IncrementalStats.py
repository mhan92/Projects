# -*- coding: utf-8 -*-
"""
Created on Mon Jul 23 15:52:16 2018

@author: hanm1
"""

import os
import xml.etree.ElementTree as et
import pandas as pd
import pyodbc
import math
from datetime import datetime, timedelta, date
import time

start = time.time()

## GOAL: Monthly ran script that calculates average and standard devation record load. Based on the log table and the "INSERTED_REC_COUNT"

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
start_date = root.find('DATABASE/startDate').text

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
freqRef = freqRef.loc[(freqRef['FREQ'] == 'daily') | (freqRef['FREQ'] == 'monthly') |
                      (freqRef['FREQ'] == 'weekly') | (freqRef['FREQ'] == 'quarterly')]

# load previous month's stats for incremental analysis
prevStats = pd.read_csv('table_statistics_{}_ALL.csv'.format(db))

prevStats = prevStats.loc[prevStats['RECORD_COUNT_STDDEV'].notnull()]

############################
## INCREMENTAL STATISTICS ##
############################

# grab first date statistics have been ran and format it for analysis
firstDay = str(prevStats.sort_values('DATE_COLLECTED').DATE_COLLECTED.iloc[0])
firstDay = datetime.strptime(firstDay, '%Y%m%d').date()

# most current date from previous month to calculate total number of periods so far then add current period to increment mean and stddev
lastDay = str(prevStats.sort_values('DATE_COLLECTED', ascending = False).DATE_COLLECTED.iloc[0])
lastDay = datetime.strptime(lastDay, '%Y%m%d').date()

# number of days total
dayTimeDiff = lastDay - firstDay

# loop through multiple log tables within database
for tbl in freqRef['TABLE_NAME']:
    
    headersSQL = 'select * from {}..{} limit 1'.format(db, tbl)
    headers_list = pd.read_sql_query(headersSQL, conn).columns.tolist()
        
    # Separate out DM_CREATED, DM_MODIFIED, and DM_LOADED
    dateCol = [col for col in headers_list if 'DM_C' in col]
        
    # Only look at DATE or DT columns instead of BY
    dateCol_dt = [col for col in dateCol if '_DATE' in col or '_DT' in col]
    
    # Put lists into one string for SQL Query
    dateCol_string = ', '.join(dateCol_dt)
    
    # SQL Query to pull all the DM_CREATED, MODIFIED, and LOAD DATE/DT columns based on the DM_CREATED date for current
    dateSQL = '''select {0} from {1}..{2}
                where {3} > TO_DATE('{4}', 'yyyymmdd') 
                group by {0}'''.format(dateCol_string, db, tbl, dateCol_dt[0], start_date)
    dateDf = pd.read_sql_query(dateSQL, conn)

    # day #
    
    if freqRef.loc[freqRef['TABLE_NAME'] == tbl]['FREQ'].iloc[0] == 'daily':
    
        # previous month's daily average
        prevDayAvg = prevStats.loc[prevStats['TABLE_NAME'] == tbl]['RECORD_COUNT_AVG'].iloc[0]
        # previous month's daily stddev
        prevDayStddev = prevStats.loc[prevStats['TABLE_NAME'] == tbl]['RECORD_COUNT_STDDEV'].iloc[0]
    
        
        # sql query for the netezza total count
        sql = """select date({3}), count(1) from {0}..{1} 
                 where date({3}) > TO_DATE('{2}', 'yyyymmdd')
                 group by date({3})""".format(db, tbl, lastDay, dateCol_dt[0])
        day_df = pd.read_sql_query(sql, conn)
    
        # incremental statistics calc
        ## loop through each date in the log
        for day in day_df['DATE']:
            
            # number of observations with current and past dates included
            n = (dayTimeDiff + (curr_date - lastDay)).days
            
            # incremental Average
            # https://math.stackexchange.com/questions/106700/incremental-averageing
            newDayAvg = prevDayAvg + ( day_df.loc[day_df['DATE'] == day].iloc[0][1] - prevDayAvg ) / n
        
            # incremental stddev calculation
            # http://datagenetics.com/blog/november22017/index.html
            newDayVar = ( prevDayStddev ) ** 2 + ( day_df.loc[day_df['DATE'] == day].iloc[0][1] - prevDayAvg )( day_df.loc[day_df['DATE'] == day].iloc[0][1] - newDayAvg )
            
            # square root of variance provide standard deviation
            newDayStddev = math.sqrt( newDayVar / n)
            
            # replace previous statistics in the loop
            prevDayAvg = newDayAvg
            prevDayStddev = newDayStddev
            
        # new incremental average
        incDayAvg = prevDayAvg
        incDayStddev = prevDayStddev
        
        # append to new stats to table
        stat_sum = stat_sum.append({'TABLE_NAME': tbl, 'RECORD_COUNT_AVG': incDayAvg, 'RECORD_COUNT_STDDEV': incDayStddev, 
                                    'DATE_COLLECTED': dateformat, 'FREQ': 'daily'}, ignore_index=True)
    
    elif freqRef.loc[freqRef['TABLE_NAME'] == tbl]['FREQ'].iloc[0] == 'weekly':
        
        # previous month's week average
        prevWeekAvg = prevStats.loc[prevStats['TABLE_NAME'] == tbl]['RECORD_COUNT_AVG'].iloc[0]
        
        # previous month's week stddev
        prevWeekStddev =  prevStats.loc[prevStats['TABLE_NAME'] == tbl]['RECORD_COUNT_STDDEV'].iloc[0]
        
        # sql query for the netezza total count
        sql = """select date({3}), count(1) from {0}..{1} 
                 where date({3}) > TO_DATE('{2}', 'yyyymmdd')
                 group by date({3})""".format(db, tbl, lastDay, dateCol_dt[0])
        week_df = pd.read_sql_query(sql, conn)
        
        # weekly count from log table
        newWeekCount = week_df.groupby([pd.Grouper(key='DATE', freq='W')])['COUNT'].sum().reset_index().sort_values('DATE')
        
        # incremental statistics calc
        ## loop through each date in the log
        for week in newWeekCount['DATE']:
            
            # number of weeks past
            n = ((dayTimeDiff + (curr_date - lastDay)).days) / 7
            
            # incremental Average
            # https://math.stackexchange.com/questions/106700/incremental-averageing
            newWeekAvg = prevWeekAvg + ( newWeekCount.loc[newWeekCount['DATE'] == week].iloc[0][1] - prevWeekAvg ) / n
        
            # incremental stddev calculation
            # http://datagenetics.com/blog/november22017/index.html
            newWeekVar = ( prevWeekStddev ) ** 2 + ( newWeekCount.loc[newWeekCount['DATE'] == week].iloc[0][1] - prevWeekAvg )( newWeekCount.loc[newWeekCount['DATE'] == week].iloc[0][1] - newWeekAvg )
            
            # square root of variance provide standard deviation
            newWeekStddev = math.sqrt(newWeekVar / n)
            
            # replace previous statistics in the loop
            prevWeekAvg = newWeekAvg
            prevWeekStddev = newWeekStddev
            
        # new incremental average
        incWeekAvg = prevWeekAvg
        incWeekStddev = prevWeekStddev
    
        # append incremental stats to stats table
        stat_sum = stat_sum.append({'TABLE_NAME': tbl, 'RECORD_COUNT_AVG': incWeekAvg, 'RECORD_COUNT_STDDEV': incWeekStddev, 
                                    'DATE_RAN': dateformat, 'FREQ': 'weekly'}, ignore_index=True)
    
    # month #
    
    elif freqRef.loc[freqRef['TABLE_NAME'] == tbl]['FREQ'].iloc[0] == 'monthly':
        
        # previous month's month average
        prevMonthAvg = prevStats.loc[prevStats['TABLE_NAME'] == tbl]['RECORD_COUNT_AVG'].iloc[0]
        
        # previous month's month stddev
        prevMonthStddev =  prevStats.loc[prevStats['TABLE_NAME'] == tbl]['RECORD_COUNT_STDDEV'].iloc[0]
        
        # sql query for the netezza count
        sql = """select date({3}), count(1) from {0}..{1} 
                 where date({3}) > TO_DATE('{2}', 'yyyymmdd')
                 group by date({3})""".format(db, tbl, lastDay, dateCol_dt[0])
        month_df = pd.read_sql_query(sql, conn)
        
        # weekly count from log table
        newMonthCount = month_df.groupby([pd.Grouper(key='DATE', freq='M')])['COUNT'].sum().reset_index().sort_values('DATE')
        
        # incremental statistics calc
        ## loop through each date in the log
        for month in newMonthCount['DATE']:
            
            # number of months past
            n = ((dayTimeDiff + (curr_date - lastDay)).days) / 30
            
            # incremental Average
            # https://math.stackexchange.com/questions/106700/incremental-averageing
            newMonthAvg = prevMonthAvg + ( newMonthCount.loc[newMonthCount['DATE'] == month].iloc[0][1] - prevMonthAvg ) / n
        
            # incremental stddev calculation
            # http://datagenetics.com/blog/november22017/index.html
            newMonthVar = ( prevMonthStddev ) ** 2 + ( newMonthCount.loc[newMonthCount['DATE'] == month].iloc[0][1] - prevMonthAvg )( newMonthCount.loc[newMonthCount['DATE'] == month].iloc[0][1] - newMonthAvg )
            
            # square root of variance provide standard deviation
            newMonthStddev = math.sqrt( newMonthVar / n)
            
            # replace previous statistics in the loop
            prevMonthAvg = newMonthAvg
            prevMonthStddev = newMonthStddev
            
        # new incremental average
        incMonthAvg = prevMonthAvg
        incMonthStddev = prevMonthStddev
        
        # append incremental stats to stats table
        stat_sum = stat_sum.append({'TABLE_NAME': tbl, 'RECORD_REC_MEAN': incMonthAvg, 'RECORD_COUNT_STDDEV': incMonthStddev, 
                                    'DATE_RAN': dateformat, 'FREQ': 'monthly'}, ignore_index=True)
    
    
    
    # quarter #
    
    elif freqRef.loc[freqRef['TABLE_NAME'] == tbl]['FREQ'].iloc[0] == 'quarterly':
        
        # previous month's quarter average
        prevQuartAvg = prevStats.loc[prevStats['TABLE_NAME'] == tbl]['RECORD_COUNT_AVG'].iloc[0]
        
        # previous month's quarter stddev
        prevQuartStddev =  prevStats.loc[prevStats['TABLE_NAME'] == tbl]['RECORD_COUNT_STDDEV'].iloc[0]
        
        # sql query for the netezza count
        sql = """select date({3}), count(1) from {0}..{1} 
                 where date({3}) > TO_DATE('{2}', 'yyyymmdd')
                 group by date({3})""".format(db, tbl, lastDay, dateCol_dt[0])
        quarter_df = pd.read_sql_query(sql, conn)
        
        # weekly count from log table
        newQuarterCount = quarter_df.groupby([pd.Grouper(key='DATE', freq='BQ-MAR')])['COUNT'].sum().reset_index().sort_values('DATE')
        
        # incremental statistics calc
        ## loop through each date in the log
        for quarter in newQuarterCount['DATE']:
            
            # number of months past
            n = ((dayTimeDiff + (curr_date - lastDay)).days) / 90
            
            # incremental Average
            # https://math.stackexchange.com/questions/106700/incremental-averageing
            newQuartAvg = prevQuartAvg + ( newQuarterCount.loc[newQuarterCount['DATE'] == quarter].iloc[0][1] - prevQuartAvg ) / n
        
            # incremental stddev calculation
            # http://datagenetics.com/blog/november22017/index.html
            newQuartVar = ( prevQuartStddev ) ** 2 + ( newQuarterCount.loc[newQuarterCount['DATE'] == quarter].iloc[0][1] - prevQuartAvg )( newQuarterCount.loc[newQuarterCount['DATE'] == quarter].iloc[0][1] - newQuartAvg )
            
            # square root of variance provide standard deviation
            newQuartStddev = math.sqrt( newQuartVar / n)
            
            # replace previous statistics in the loop
            prevQuartAvg = newQuartAvg
            prevQuartStddev = newQuartStddev
            
        # new incremental average
        incQuartAvg = prevQuartAvg
        incQuartStddev = prevQuartStddev
        
        # append incremental stats to stats table
        stat_sum = stat_sum.append({'TABLE_NAME': quarter, 'RECORD_REC_MEAN': incQuartAvg, 'RECORD_REC_STDDEV': incQuartStddev, 
                                    'DATE_RAN': dateformat, 'FREQ': 'quarterly'}, ignore_index=True)
    

# put results of the table scanning and append the results to the exisiting CSV file.
# should transition into uploading a SQL table within Netezza
with open('table_statistics_{}_ALL.csv'.format(db), 'a') as log:
    print('Results have been saved to existing CSV file\n', os.getcwd())
    stat_sum.to_csv(log, header = False, index = False)
log.close()

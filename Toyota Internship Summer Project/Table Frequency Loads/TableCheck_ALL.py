# -*- coding: utf-8 -*-
"""
Created on Mon Jun 25 15:29:34 2018

@author: hanm1
"""

import os
import xml.etree.ElementTree as et
import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime, timedelta, date


## GOAL: Daily table check that uses previous month's statistics to determine whether a table recieved less or greater 
##       2 standard deviations from the mean
## code is broken up into daily, weekly, monthly, and quarterly which is why the code is very long.
## reasoning behind it is for easier debugging and ease for applying different logic to specific table frequencies
### the code can be better optimized by implementing a for loop possibly

# change working directory to retrieve configuration xml file values
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

# Initial Date is the date if an historical analysis would be used
## everything after initial date to current date will be pulled from netezza 
### the script will flag which specific period/date falls outside of the average
initial_date = root.find('DATABASE/histDate').text

# formats the initial date for output purposes in YYYY-MM-DD format
initial_date_format = datetime.strptime(initial_date, '%Y%m%d').strftime('%Y-%m-%d')

# Start date is different from the initial date
## used for pulling the log table into the script
### the dates from the log table are used to determine whether a table should have recieved records for it's frequency
#### this date has to be the same date used for the TableMonthlyStats.py script
start_date = root.find('DATABASE/startDate').text

# Automatically determine the current date for script when it runs 
## for analysis purposes, we will use current day minus 1
today = date.today()

# checks if current date is on a monday
if today.weekday() == 0:
    
    # if monday, pull table records ending on Friday
    curr_date  = today - timedelta(days=3)
else:
    
    # any other day will be current day - 1
    curr_date = (today - timedelta(days=1))

# formats the current date for output purposes
dateformat = curr_date.strftime('%Y%m%d')

# Pulls Mart and log table from the xml file
## the reason there's a split is if there are multiple log tables within a mart
db = (root.find('DATABASE/db').text)
tbl = (root.find('DATABASE/TABLE/tbl').text)


##########################
### NETEZZA CONNECTION ###
##########################

# connection string that connects the script to Netezza and settings
conn = pyodbc.connect("DRIVER={NetezzaSQL};SERVER="+hostname+";PORT="+port+";DATABASE="+database+
                      ";UID="+uid+";PWD="+root.find('Netezza/pwd').text+";DSN=NZSQL")
cursor = conn.cursor()

#####################
### PROGRAM START ###
#####################

# empty dataframe to store the results and any new records a previously zero record table gets
results = pd.DataFrame()
new = pd.DataFrame()

# strips any white spaces that may be in the xml file
tbl = tbl.strip()

# loads in the frequency, statistics, and zero reference csvs that TableMonthlyStats.py and tablefrequency.py outputs
freqRef = pd.read_csv('table_frequency_{}_ALL.csv'.format(db))
statRef = pd.read_csv('table_statistics_{}_ALL.csv'.format(db))

# Next few lines pulls the most recent stats from the statsRef csv
## The monthly stats has the capability to update statistics incrementally, as a result, we will need to most recent stats for our analysis
statRef['DATE_COLLECTED'] = pd.to_datetime(statRef['DATE_COLLECTED'], format = '%Y%m%d')

# sorts statsRef.csv by DATE_RAN ascending
statRef = statRef.sort_values('DATE_COLLECTED', ascending = False)

# Grabs the first row's DATE
recentDate = statRef.iloc[0][0]

# subsets the statRef csv by the most recent date it finds
statRef = statRef.loc[statRef['DATE_COLLECTED'] == recentDate]

print('\n Table Check for {} \n'.format(db))

# filters program to only look for specific tables
if (len(tbl) != 0) & (tbl != '*'):
    statRef = statRef.loc[statRef['TABLE_NAME'] == tbl]

# pass over if * is given
elif tbl == '*':
    pass

statRef = statRef.loc[statRef['TABLE_NAME'].str.startswith('FSV_A')]
#############################################
### TABLES WITH ZERO RECORDS HISTORICALLY ###
#############################################

# print statment to section the output for readability
print('\n Tables with completely new records loaded:\n')

# subset statRef dataframe for tables that having missing or 0 averages
zeroRef = statRef.loc[(statRef['RECORD_COUNT_AVG'] == 0) | (statRef['RECORD_COUNT_AVG'].isnull())]

# iterates through the table names from the zeroRef csv and runs a sql query to see if there are any new records a table recieved
for z in zeroRef['TABLE_NAME']:
    
    try:
        # SQL query to pull the counts from the zero table
        zeroSql = 'select count(1) from {}..{}'.format(db, z)
        zeroDf = pd.read_sql_query(zeroSql, conn)
        
        # condition statment to output a message with the table name, count, and date the flag occured
        if zeroDf['COUNT'].values !=0:
            
            # refers back to existing csv file with previous' day new tables
            if os.path.exists('TABLE_NEW_RECORDS_{0}..{1}.csv'.format(db, tbl)):
                
                # loads in previous day's table
                newRef = pd.read_csv('TABLE_NEW_RECORDS_{0}..{1}.csv'.format(db, tbl))
                
                # puts table names into list
                newRef_list = newRef['TABLE_NAME'].tolist()
                
                # checks if table is in the list and flags the table for having new records
                if z not in newRef_list:
                    print('{}..{} has {} new observations for {}'.format(db, z, zeroDf['COUNT'].values, curr_date))
                    
                    # puts results into a format that can be appended
                    new = new.append({'TABLE_NAME': z, 'RECORD_COUNT': zeroDf['COUNT'][0], 'DATE_RAN': dateformat}, ignore_index=True)
                     
                    # append the new tables to exisiting csv file
                    with open('TABLE_CHECK_LOG_{0}..{1}.csv'.format(db, tbl), 'a') as log:
                         results.to_csv(log, header = False, index = False)
                    log.close()
            
            # print statment declaring tables with new records
            print('{}..{} has {} new observations for {}'.format(db, z, zeroDf['COUNT'].values, curr_date))
            
            # saves the output to the empty dataframe 'new'
            new = new.append({'TABLE_NAME': z, 'RECORD_COUNT': zeroDf['COUNT'][0], 'DATE_RAN': dateformat}, ignore_index=True)
    except:
        pass

if len(new) == 0:
    print('No Tables have been flagged for new records \n')
# saves the 'new' dataframe for analysis and record keeping
new.to_csv('TABLE_NEW_RECORDS_{0}.csv'.format(db), index=False)

###################
### TABLE CHECK ###
###################

### DAILY ###

# print statement to section off the output
print('\n Daily Frequency Check from {} to {}: \n'.format(initial_date_format, curr_date))
    
# pulls the statistics for tables that are classified "daily"
dayRef = statRef.loc[statRef['FREQ'] =='daily']

# iterates through the daily tables that we have statistics on
for x in dayRef['TABLE_NAME']:
    
    # Simple SQL query to determine the "date" column used to find out the counts by days
    dateDayColSql = 'select * from {}..{} limit 1'.format(db, x)
    dateDayColHeaders = pd.read_sql_query(dateDayColSql, conn).columns.tolist()
    
    # depending on the database, different date columns will be pulled
    if db == 'HUB': # hub based on AS_OF_DATE
        dateCol = [col for col in dateDayColHeaders if 'AS_OF' in col]
        day_date = [col for col in dateCol if '_DATE' in col or '_DT' in col][0]
    elif db == 'DM_CUSTOMER': # dm_customer based on dm_created_date
        # pull column headers DM_CREATED, DM_MODIFIED, and DM_LOAD
        dateCol = [col for col in dateDayColHeaders if 'DM_C' in col]
        day_date = [col for col in dateCol if '_DATE' in col or '_DT' in col][0]
    else: # everything else based on load or as_of date
        dateCol = [col for col in dateDayColHeaders if 'LOAD' in col or 'AS_OF' in col]
        day_date = [col for col in dateCol if '_DATE' in col or '_DT' in col][0]

    
    # daySql groups the count by DM_CREATED_DATE
    daySql = '''select DATE({2}), count(1) from {0}..{1} 
                where DATE({2}) between TO_DATE('{3}', 'yyyymmdd') and TO_DATE('{4}', 'yyyymmdd')
                group by {2}'''.format(db, x, day_date, initial_date, dateformat)
    dayCntDf = pd.read_sql_query(daySql, conn)
    
    # force date column to datetime variable
    dayCntDf['DATE'] = pd.to_datetime(dayCntDf['DATE'])
    
    # group Netezza dataframe to daily
    dayGroup = dayCntDf.groupby([pd.Grouper(key='DATE', freq = 'D')])['COUNT'].sum()
    
    # pulls average and stddev from the stats ref by table name 
    dayAvg = dayRef.loc[dayRef['TABLE_NAME'] == x]['RECORD_COUNT_AVG'].iloc[0]
    dayStddev = dayRef.loc[dayRef['TABLE_NAME'] == x]['RECORD_COUNT_STDDEV'].iloc[0]

    # ignoring tables that have null standard deviations, experienced issues within Dev Environment
    if np.isnan(dayStddev) == False:
        
        # iterates through each date from the daySql query
        ## if a date range was given, the script should flag which dates fal outside the average
        for idx in dayGroup.index:
            
            # checks if the date scaned occurs on a saturday or sunday and pass over it
            if (idx.weekday() != 6) and (idx.weekday() != 7):
                # flagging tables that have 0 records for the day
                if dayGroup.loc[idx] == 0:
                    print('{} daily table has zero records for {}'.format(x, idx.date()))
                    
                    # Assigns the corresponding flag, average, date of flag, and count on date
                    res = dayRef.loc[dayRef['TABLE_NAME'] == x].iloc[0]
                    res['FLAG'] = 'ZERO RECORDS'
                    res['RECORD_COUNT_AVG'] = dayAvg
                    res['ERR_DATE'] = idx.date()
                    res['COUNT'] = dayGroup[idx]
                    
                    # appends the new row to the empty dataframe
                    results = results.append(res)
                
                # flagging tables that are greater than the average
                elif dayGroup.loc[idx] > dayAvg + 2 * dayStddev:
                    print('{} daily table has more than expected for {}'.format(x, idx.date()))
                    
                    # Assigns the corresponding flag, average, date of flag, and count on date
                    res = dayRef.loc[dayRef['TABLE_NAME'] == x].iloc[0]
                    res['FLAG'] = 'GREATER'
                    res['RECORD_COUNT_AVG'] = dayAvg
                    res['ERR_DATE'] = idx.date()
                    res['COUNT'] = dayGroup[idx]
                    
                    # appends the new row to the empty dataframe
                    results = results.append(res)
                    
                # flagging tables that are less than the average
                elif dayGroup.loc[idx] < dayAvg - 2 * dayStddev:
                    print('{} daily table has less than expected for {}'.format(x, idx.date()))
                    
                    # Assigns the corresponding flag, average, date of flag, and count on date
                    res = dayRef.loc[dayRef['TABLE_NAME'] == x].iloc[0]
                    res['FLAG'] = 'LESS'
                    res['RECORD_COUNT_AVG'] = dayAvg
                    res['ERR_DATE'] = idx.date()
                    res['COUNT'] = dayGroup[idx]
                    
                    # appends the new row to the empty dataframe
                    results = results.append(res)
    
    # Condition statement for tables that have null standard deviation but there was a calculated average
    ## similar to the code above except the comparison is done strictly against the average with no standard error
    elif (np.isnan(dayStddev) == True) & (dayAvg != 0):
        for idx in dayGroup.index:
            # checks if the date scaned occurs on a saturday or sunday and pass over it
            if (idx.weekday() != 6) and (idx.weekday() != 7):
                
                if dayGroup.loc[idx] == 0:
                    print('{} daily table has zero records for {}'.format(x, idx.date()))
    
                    res = dayRef.loc[dayRef['TABLE_NAME'] == x].iloc[0]
                    res['FLAG'] = 'ZERO RECORDS'
                    res['RECORD_COUNT_AVG'] = dayAvg
                    res['ERR_DATE'] = idx.date()
                    res['COUNT'] = dayGroup[idx]
                    results = results.append(res)
    
                elif dayGroup.loc[idx] > dayAvg:
                    print('{} daily table is greater than the average of on {}'.format(x, idx.date()))
    
                    res = dayRef.loc[dayRef['TABLE_NAME'] == x].iloc[0]
                    res['FLAG'] = 'GREATER'
                    res['RECORD_COUNT_AVG'] = dayAvg
                    res['ERR_DATE'] = idx.date()
                    res['COUNT'] = dayGroup[idx]
                    results = results.append(res)
    
                elif dayGroup.loc[idx] < dayAvg:
                    print('{} daily table has less than the average for {}'.format(x, idx.date()))
                    
                    res = dayRef.loc[dayRef['TABLE_NAME'] == x].iloc[0]
                    res['FLAG'] = 'LESS'
                    res['RECORD_COUNT_AVG'] = dayAvg
                    res['ERR_DATE'] = idx.date()
                    res['COUNT'] = dayGroup[idx]
                    results = results.append(res)

    
### WEEKLY ###
 
# section break for weekly tables in output
print('\n Weekly Frequency Check from {} to {}: \n'.format(initial_date_format, curr_date))

# subset statRef for weekly tables
weekRef = statRef.loc[statRef['FREQ'] =='weekly']

# iterate through tables classified as weekly updated
for w in weekRef['TABLE_NAME']:
    
    # determine which column contains the date to group by
    dateWeekColSql = 'select * from {}..{} limit 1'.format(db, w)
    dateWeekColHeaders = pd.read_sql_query(dateWeekColSql, conn).columns.tolist()

    # depending on the database, different date columns will be pulled
    if db == 'HUB': # hub based on AS_OF_DATE
        dateCol = [col for col in dateWeekColHeaders if 'AS_OF' in col]
        week_date = [col for col in dateCol if '_DATE' in col or '_DT' in col][0]
    elif db == 'DM_CUSTOMER': # dm_customer based on dm_created_date
        # pull column headers DM_CREATED, DM_MODIFIED, and DM_LOAD
        dateCol = [col for col in dateWeekColHeaders if 'DM_C' in col]
        week_date = [col for col in dateCol if '_DATE' in col or '_DT' in col][0]
    else: # everything else based on load or as_of date
        dateCol = [col for col in dateWeekColHeaders if 'LOAD' in col or 'AS_OF' in col]
        week_date = [col for col in dateCol if '_DATE' in col or '_DT' in col][0]
    
    # determine the last date that recieved records
    lastDateSQL = 'select max(date({0})) from {1}..{2}'.format(week_date, db, w)
    week_last_ran = pd.read_sql_query(lastDateSQL, conn).iloc[0][0]

        # pull weekly averages and standard deviations from statRef
    weekAvg = weekRef.loc[weekRef['TABLE_NAME'] == w]['RECORD_COUNT_AVG'].iloc[0]
    weekStddev = weekRef.loc[weekRef['TABLE_NAME'] == w]['RECORD_COUNT_STDDEV'].iloc[0]

    # checks if the last date ran for weekly is within 4-8 days of the current date otherwise flag for not meeting the update frequency
    if ((curr_date - week_last_ran > timedelta(days = 4)) & (curr_date - week_last_ran <= timedelta(days = 8))):    

        # weekSql groups the count by DM_CREATED_DATE
        weekSql = '''select DATE({2}), count(1) from {0}..{1} 
                    where DATE({2}) between TO_DATE('{3}', 'yyyymmdd') and TO_DATE('{4}', 'yyyymmdd')
                    group by {2}'''.format(db, x, week_date, initial_date, dateformat)
        weekCntDf = pd.read_sql_query(daySql, conn)
    
        # force date column to datetime variable
        weekCntDf['DATE'] = pd.to_datetime(dayCntDf['DATE'])
            
        # group the counts from Netezza by week
        weekGroup = weekCntDf.groupby([pd.Grouper(key='DATE', freq = 'W')])['COUNT'].sum()    
    
        
        # Again, ran into an issue of null standard deviations within the dev environment
        if np.isnan(weekStddev) == False:
            
            # iterate though the weekly dates 
            for idx in weekGroup.index.date:
                
                # flag tables that have zero records for the week
                if weekGroup.loc[idx] == 0:
                    print('{} weekly table has zero records for the week of {}'.format(w, idx))

                    # put the weekly average, count, flag, and when the flag was raised into a series
                    res = weekRef.loc[weekRef['TABLE_NAME'] == w].iloc[0]
                    res['FLAG'] = 'ZERO RECORDS'
                    res['RECORD_COUNT_AVG'] = weekAvg
                    res['ERR_DATE'] = idx
                    res['COUNT'] = weekGroup.loc[idx]
                    
                    # append series to a dataframe for analysis
                    results = results.append(res)
                    
                # flag tables that were greater than the average and standard deviation
                elif weekGroup.loc[idx] > weekAvg + 2 * weekStddev:
                    print('{} has more than expected for week of {}'.format(w, idx))

                    # put weekly average, count, flag and when the flag was raise into series
                    res = weekRef.loc[weekRef['TABLE_NAME'] == w].iloc[0]
                    res['FLAG'] = 'GREATER'
                    res['RECORD_COUNT_AVG'] = weekAvg
                    res['ERR_DATE'] = idx
                    res['COUNT'] = weekGroup.loc[idx]
                    
                    # append series to dataframe
                    results = results.append(res)
                
                # flag tables that were less than the average and standard deviation 
                elif weekGroup.loc[idx] < weekAvg - 2 * weekStddev:
                    print('{} has less than expected for the week of {}'.format(w, idx))

                    # put weekly average, count, flag and when the flag was raised into series
                    res = weekRef.loc[weekRef['TABLE_NAME'] == w].iloc[0]
                    res['FLAG'] = 'LESS'
                    res['RECORD_COUNT_AVG'] = weekAvg
                    res['ERR_DATE'] = idx
                    res['COUNT'] = weekGroup.loc[idx]
                    
                    # append series to dataframe
                    results = results.append(res)
                
        # similar to code above but only looking at averages due to standard devation being misssing
        ## can be removed during production if situation never arises
        elif (np.isnan(weekStddev) == True) & (weekAvg != 0) | (np.isnan(weekAvg) == False): 
            for idx in weekGroup.index.date:
                
                    if weekGroup.loc[idx] == 0:
                        print('{} weekly table has zero records for the week of {}'.format(w, idx))
                        res = weekRef.loc[weekRef['TABLE_NAME'] == w].iloc[0]
                        res['FLAG'] = 'ZERO RECORDS'
                        res['RECORD_COUNT_AVG'] = weekAvg
                        res['ERR_DATE'] = idx
                        res['COUNT'] = weekGroup.loc[idx]
                        results = results.append(res)

                    elif weekGroup.loc[idx] > weekAvg:
                        print('{} has more than expected for {}'.format(w, idx))
                        res = weekRef.loc[weekRef['TABLE_NAME'] == w].iloc[0]
                        res['FLAG'] = 'GREATER'
                        res['RECORD_COUNT_AVG'] = weekAvg
                        res['ERR_DATE'] = idx.date()
                        res['COUNT'] = weekGroup.loc[idx]
                        results = results.append(res)

                    elif weekGroup.loc[idx] < weekAvg:
                        print('{} has less than expected for {}'.format(w, idx))
                        res = weekRef.loc[weekRef['TABLE_NAME'] == w].iloc[0]
                        res['FLAG'] = 'LESS'
                        res['RECORD_COUNT_AVG'] = weekAvg
                        res['ERR_DATE'] = idx
                        res['COUNT'] = weekGroup.loc[idx]
                        results = results.append(res)

    # raises flag for tables that did not recieve records during expected time period
    elif curr_date - week_last_ran > timedelta(days = 8):
        print('{} weekly table has not recieve any records for {}'.format(w, curr_date))
        
        # put weekly average, count, flag and when the flag was raised into series
        res = weekRef.loc[weekRef['TABLE_NAME'] == w].iloc[0]
        res['FLAG'] = 'NOT RAN FOR PERIOD'
        res['RECORD_COUNT_AVG'] = weekAvg
        res['ERR_DATE'] = idx
        res['COUNT'] = 'NONE'
        
        # appends series into dataframe
        results = results.append(res)


### MONTHLY ###

# print statement to section off monthly results
print('\n Table Monthly Frequency Check from {} to {}: \n'.format(initial_date_format, curr_date))

# load in monthly statistics
monthRef = statRef.loc[statRef['FREQ'] =='monthly']

# iterate through tables that have statistics on
for m in monthRef['TABLE_NAME']:
    
    # simple SQL query to determine the date columns
    dateMonthColSql = 'select * from {}..{} limit 1'.format(db, m)
    dateMonthColHeaders = pd.read_sql_query(dateMonthColSql, conn).columns.tolist()
        
    # put the columns into a list and pull out DM_CREATED_BY/DATE/DT 
    month_date_header_list = [col for col in dateMonthColHeaders if 'DM_C' in col]
    
    # depending on the database, different date columns will be pulled
    if db == 'HUB': # hub based on AS_OF_DATE
        dateCol = [col for col in dateMonthColHeaders if 'AS_OF' in col]
        month_date = [col for col in dateCol if '_DATE' in col or '_DT' in col][0]
    elif db == 'DM_CUSTOMER': # dm_customer based on dm_created_date
        # pull column headers DM_CREATED, DM_MODIFIED, and DM_LOAD
        dateCol = [col for col in dateMonthColHeaders if 'DM_C' in col]
        month_date = [col for col in dateCol if '_DATE' in col or '_DT' in col][0]
    else: # everything else based on load or as_of date
        dateCol = [col for col in dateMonthColHeaders if 'LOAD' in col or 'AS_OF' in col]
        month_date = [col for col in dateCol if '_DATE' in col or '_DT' in col][0]
    
    # determine the last date that recieved records
    lastDateSQL = 'select max(date({0})) from {1}..{2}'.format(month_date, db, m)
    month_last_ran = pd.read_sql_query(lastDateSQL, conn).iloc[0][0]

    # pull average/standard deviation from statsRef for analysis
    monthAvg = monthRef.loc[monthRef['TABLE_NAME'] == m]['RECORD_COUNT_AVG'].iloc[0]
    monthStddev = monthRef.loc[monthRef['TABLE_NAME'] == m]['RECORD_COUNT_STDDEV'].iloc[0]

    # if the last_run date and the current date is within 29-32 days, the analysis is ran otherwise, raise a flag
    if (curr_date - month_last_ran > timedelta(days=29)) & (curr_date - month_last_ran <= timedelta(days=32)):

        # Monthly count SQL by the month_date_id to group by
        monthCntSql = '''select DATE({2}), count(1) from {0}..{1} 
                        where DATE({2}) between TO_DATE('{3}', 'yyyymmdd') and TO_DATE('{4}', 'yyyymmdd')
                        group by DATE({2})'''.format(db, m, month_date, initial_date, dateformat)
        monthDf = pd.read_sql_query(monthCntSql, conn)
        
        # group counts by month
        monthGroup = monthDf.groupby([pd.Grouper(key='DATE', freq = 'M')])['COUNT'].sum()
    
        # iterate through month's in the netezza count
        for idx in monthGroup.index.date:

            # flag tables that have zero records for the month
            if (monthGroup.loc[idx] == 0):
                print('{} has zero records for {}'.format(m, idx))
                
                # put the month count, average, flag, and when the flag occured
                res = monthRef.loc[monthRef['TABLE_NAME'] == m].iloc[0]
                res['FLAG'] = 'ZERO RECORDS'
                res['RECORD_COUNT_AVG'] = monthAvg
                res['ERR_DATE'] = idx.date()
                res['COUNT'] = monthGroup.loc[idx]

                # append results to the results dataframe
                results = results.append(res)
    
            # flag tables that are greater than the average
            elif  monthGroup.loc[idx] > monthAvg + 2 * monthStddev:
                print('{} has more than expected for {}'.format(m, idx))
                
                # put month count, average, flag and when the flag occured
                res = monthRef.loc[monthRef['TABLE_NAME'] == m].iloc[0]
                res['FLAG'] = 'GREATER'
                res['RECORD_COUNT_AVG'] = monthAvg
                res['ERR_DATE'] = idx.date()
                res['COUNT'] = monthGroup.loc[idx]
                
                # append results to empty dataframe
                results = results.append(res)
            
            # flag tables that are less than the average
            elif monthGroup.loc[idx] < monthAvg - 2 * monthStddev:
                print('{} has less than expected for {}'.format(m, idx))
                
                # put month count, average, flag and when the flag occured
                res = monthRef.loc[monthRef['TABLE_NAME'] == m].iloc[0]
                res['FLAG'] = 'LESSER'
                res['RECORD_COUNT_AVG'] = monthAvg
                res['ERR_DATE'] = idx.date()
                res['COUNT'] = monthGroup.loc[idx]
                
                # apped results to results dataframe
                results = results.append(res)

    # flag table if there were no records still after the time period has past
    elif curr_date - month_last_ran > timedelta(days = 32):
        print('{} monthly table has not recieve any records for {}'.format(m, curr_date))

        # put month count, average, flag and when the flag occured
        res = monthRef.loc[monthRef['TABLE_NAME'] == m].iloc[0]
        res['FLAG'] = 'NOT RAN FOR PERIOD'
        res['RECORD_COUNT_AVG'] = monthAvg
        res['ERR_DATE'] = idx
        res['COUNT'] = 'NONE'
        
        # append results to empty dataframe
        results = results.append(res)


## QUARTERLY ##

# subset quarterly results for output
print('\n Table Quarterly Frequency Check from {} to {}: \n'.format(initial_date_format, curr_date))

# subset statRef for quarterly tables
quarterRef = statRef.loc[statRef['FREQ'] =='quarterly']

# iterate through quarterly tables that have statistics 
for q in quarterRef['TABLE_NAME']:
    
    # SQL query to determine the date column to group by
    dateQuartColSql = 'select * from {}..{} limit 1'.format(db, q)
    dateQuartColHeaders = pd.read_sql_query(dateQuartColSql, conn).columns.tolist()
        
    # depending on the database, different date columns will be pulled
    if db == 'HUB': # hub based on AS_OF_DATE
        dateCol = [col for col in dateQuartColHeaders if 'AS_OF' in col]
        quart_date = [col for col in dateCol if '_DATE' in col or '_DT' in col][0]
    elif db == 'DM_CUSTOMER': # dm_customer based on dm_created_date
        # pull column headers DM_CREATED, DM_MODIFIED, and DM_LOAD
        dateCol = [col for col in dateQuartColHeaders if 'DM_C' in col]
        quart_date = [col for col in dateCol if '_DATE' in col or '_DT' in col][0]
    else: # everything else based on load or as_of date
        dateCol = [col for col in dateQuartColHeaders if 'LOAD' in col or 'AS_OF' in col]
        quart_date = [col for col in dateCol if '_DATE' in col or '_DT' in col][0]
    
    # determine the last date that recieved records
    lastDateSQL = 'select max(date({0})) from {1}..{2}'.format(quart_date, db, q)
    quarter_last_ran = pd.read_sql_query(lastDateSQL, conn).iloc[0][0]

    # Get average and standard deviation from the stat ref table
    quarterAvg = quarterRef.loc[quarterRef['TABLE_NAME'] == q]['RECORD_COUNT_AVG'].iloc[0]
    quarterStddev = quarterRef.loc[quarterRef['TABLE_NAME'] == q]['RECORD_COUNT_STDDEV'].iloc[0]
        
    # if table on the last log table was ran within 89-95 days then run analysis otherwise flag the table
    if (curr_date - quarter_last_ran > timedelta(days=89)) & (curr_date - quarter_last_ran <= timedelta(days=95)):

        # Quarterly count SQL query 
        quarterCntSql = '''select DATE({2}), count(1) from {0}..{1} 
                        where DATE({2}) between TO_DATE('{3}', 'yyyymmdd') and TO_DATE('{4}', 'yyyymmdd')
                        group by DATE({2})'''.format(db, q, quart_date, initial_date, dateformat)
        quarterDf = pd.read_sql_query(quarterCntSql, conn)
        
        # Group netezza count by Quarters with quarter end on march (fiscal year)
        quarterGroup = quarterDf.groupby([pd.Grouper(key='DATE', freq = 'BQ-MAR')])['COUNT'].sum()

        # iterate through each dates in the quarter             
        for idx in quarterGroup.index.date:
            
            # flag tables that have zero records for the quarter
            if quarterGroup.loc[idx] == 0:
                print('{} has zero records for {}'.format(q, idx))

                # put the quarter's average, count, date of flag, and the flag raised
                res = quarterRef.loc[quarterRef['TABLE_NAME'] == q].iloc[0]
                res['FLAG'] = 'ZERO RECORDS'
                res['RECORD_COUNT_AVG'] = quarterAvg
                res['ERR_DATE'] = idx.date()
                res['COUNT'] = quarterGroup.loc[idx]
                
                # results into empty dataframe
                results = results.append(res)

            # flag tables that are greater than the average and standard deviation
            elif  quarterGroup.loc[idx] > quarterAvg + 2 * quarterStddev:
                print('{} has more than expected for {}'.format(q, idx))
                
                # put the quarter's average, count, date of flag, and the flag raised
                res = quarterRef.loc[quarterRef['TABLE_NAME'] == q].iloc[0]
                res['FLAG'] = 'GREATER'
                res['RECORD_COUNT_AVG'] = quarterAvg
                res['ERR_DATE'] = idx.date()
                res['COUNT'] = quarterGroup.loc[idx]
                
                # results into empty dataframe
                results = results.append(res)

            # flag tables that are less than the average and standard devation
            elif quarterGroup.loc[idx] < quarterAvg - 2 * quarterStddev:
                print('{} has less than expected for {}'.format(q, idx))
                
                # put the quarter's average, count, date of flag, and the flag raised
                res = quarterRef.loc[quarterRef['TABLE_NAME'] == q].iloc[0]
                res['FLAG'] = 'LESSER'
                res['RECORD_COUNT_AVG'] = quarterAvg
                res['ERR_DATE'] = idx.date()
                res['COUNT'] = quarterGroup.loc[idx]
                
                # results into empty dataframe
                results = results.append(res)

    # flag tables that have not recieved records during the quarter
    elif curr_date - quarter_last_ran > timedelta(days = 95):
        print('{} monthly table has not recieve any records for {}'.format(q, curr_date))

        # put the quarter's average, count, date of flag, and the flag raised
        res = quarterRef.loc[quarterRef['TABLE_NAME'] == q].iloc[0]
        res['FLAG'] = 'NOT RAN FOR PERIOD'
        res['RECORD_COUNT_AVG'] = quarterAvg
        res['ERR_DATE'] = idx
        res['COUNT'] = 'NONE'
        
        # results into empty dataframe
        results = results.append(res)                

#######################
### END OF ANALYSIS ###
#######################

# Assigns the curent date the analysis was ran for record keeping
results['DATE_RAN'] = today

# print statment if there were no flags raise for the tables
if len(results) == 0:
    print('\n No Tables Have been flagged for {} \n'.format(curr_date))
else:
    # Column order for csv to be saved with results
    results = results[['DATE_RAN','ERR_DATE','TABLE_NAME', 'FREQ', 'FLAG', 'COUNT', 'RECORD_COUNT_AVG', 'RECORD_COUNT_STDDEV']] 

    print('\n Results have been saved here:\n', os.getcwd())
    # save results to csv
    results.to_csv('TABLE_CHECK_LOG_{0}.csv'.format(db), index=False)

# Closing the connection between the script and netezza
cursor.close()
conn.close()

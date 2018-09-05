# -*- coding: utf-8 -*-
"""
Created on Thu Jul 19 13:40:30 2018

@author: hanm1
"""

import os
import xml.etree.ElementTree as et
import pandas as pd
import pyodbc
from datetime import datetime, timedelta
import time

start = time.time()

# change working directory to retrieve the XML config file
os.chdir(r'C:\Users\hanm1\Desktop\TableCheck')
os.getcwd()

##################
### XML CONFIG ###
##################

# import XML config file
tree = et.parse('config.xml')
root = tree.getroot()

# Parse through XML file
uid = root.find('Netezza/uid').text
hostname = root.find('Netezza/hostname').text
port = root.find('Netezza/port').text
database = root.find('Netezza/db').text

# pull initial start date for analysis
start_date = root.find('DATABASE/startDate').text
# formatting date for sql query
start_date_format = datetime.strptime(start_date, '%Y%m%d').date()

# pull end date for analysis
end_date = root.find('DATABASE/endDate').text
# formatting date for sql query
end_date_format = datetime.strptime(end_date, '%Y%m%d').date()

# splitting tbl, tbl_name, columns, and col_grp for multiples
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

# SQL query that pulls all the tables within the Mart
tableSQL = 'SELECT TABLE_NAME FROM {}.INFORMATION_SCHEMA.TABLES'.format(db)
df = pd.read_sql_query(tableSQL, conn)

# ignores DIMENSION tables
df = df[~df['TABLE_NAME'].str.contains('_DIM')]

# Email saying to not run any query on on VEHICLE_ATTRIBUTE_MASTER until further notice
if db == 'HUB':
    df = df.loc[df['TABLE_NAME'] != 'VEHICLE_ATTRIBUTE_MASTER']

# iterate through each table within Mart
for table in df['TABLE_NAME']:
    
    # try and except to flag tables that don't have the DM_CUSTOMER..(table) relationship in netezza
    try:
    
        # SQL Query to get table columns and determine the date columns
        headersSQL = 'select * from {}..{} limit 1'.format(db, table)
        headers_list = pd.read_sql_query(headersSQL, conn)
        
    except:
        df.loc[df['TABLE_NAME'] == table, 'FREQ'] = 'NO RELATION'
        continue
    
    # detects if the current database is DM_SALES
    if db == 'DM_SALES':
        
        # columns that end with DT or DATE
        dateCol = headers_list.loc[:,headers_list.columns.str.endswith('DATE','DT')].columns.tolist()
        dateCol_dt = [col for col in dateCol if 'AS_OF' in col]
        
        if len(dateCol_dt) == 0:
            dateCol = headers_list.loc[:,headers_list.columns.str.endswith('DATE','DT')].columns.tolist()
            dateCol_dt = [col for col in dateCol if 'DM_C' in col]
            
            if len(dateCol_dt) == 0:
                df.loc[df['TABLE_NAME'] == table, 'FREQ'] = 'NO DATE COL'
                continue
    
        # SQL Query to pull all the DM_CREATED, MODIFIED, and LOAD DATE/DT columns
        dateSQL = '''select date({0}) from {1}..{2}
                    where date({0}) between TO_DATE('{3}', 'yyyymmdd') and TO_DATE('{4}', 'yyyymmdd')
                    group by date({0})'''.format(dateCol_dt[0], db, table, start_date, end_date)
        dateDf = pd.read_sql_query(dateSQL, conn)
        
        # converts column into datetime format for analysis
        dateDf[dateDf.columns[0]] = pd.to_datetime(dateDf[dateDf.columns[0]])
    
    elif db == 'DM_CUSTOMER' :
        # Separate out date_id columns
        dateCol_dt = [col for col in headers_list if 'DATE_ID' in col]
        
        # Use different date columns for tables that don't have DATE_ID
        if len(dateCol_dt) == 0:
            dateCol = [col for col in headers_list if 'DM_C' in col]
            dateCol_dt = [col for col in dateCol if '_DATE' in col or '_DT' in col]
            
            dateSQL = '''select date({0}) from {1}..{2}
                        where date({0}) between TO_DATE('{3}', 'yyyymmdd') and TO_DATE('{4}', 'yyyymmdd')
                        group by date({0})'''.format(dateCol_dt[0], db, table, start_date, end_date)
            dateDf = pd.read_sql_query(dateSQL, conn)
            
            # Flag the table with no date columns
            if len(dateCol_dt) == 0:
                df.loc[df['TABLE_NAME'] == table, 'FREQ'] = 'NO DATE COL'
                continue
        
        
        # SQL Query for DATE_ID
        dateSQL = '''select {0} from {1}..{2}
                    where {0} between {3} and {4}
                    group by {0}'''.format(dateCol_dt[0], db, table, start_date, end_date)
        dateDf = pd.read_sql_query(dateSQL, conn)
    
        # detects if the current database is hub
    elif db == 'HUB':
        
        # in the hub database, we will only look at as_of date
        dateCol = [col for col in headers_list if 'AS_OF' in col]
        dateCol_dt = [col for col in dateCol if '_DATE' in col or '_DT' in col]
        
        if len(dateCol_dt) == 0:
            df.loc[df['TABLE_NAME'] == table, 'FREQ'] = 'NO DATE COL'
            continue
        
        else:
            # SQL Query to pull all the DM_CREATED, MODIFIED, and LOAD DATE/DT columns
            dateSQL = '''select date({0}) from {1}..{2}
                        where date({0}) between TO_DATE('{3}', 'yyyymmdd') and TO_DATE('{4}', 'yyyymmdd')
                        group by {0}'''.format(dateCol_dt[0], db, table, start_date, end_date)
            dateDf = pd.read_sql_query(dateSQL, conn)
            
            # converts column into datetime format for analysis
            dateDf[dateDf.columns[0]] = pd.to_datetime(dateDf[dateDf.columns[0]])

    
    ################################
    ### Frequency Classification ###
    ################################

    # Table flagged for only having one date
    if len(dateDf) == 1:
        df.loc[df['TABLE_NAME'] == table, 'FREQ'] = 'ONLY ONE DATE'
        continue
    # flag tables that are beyond the time period selected
    elif len(dateDf) == 0:
        df.loc[df['TABLE_NAME'] == table, 'FREQ'] = 'OUTSIDE TIME PERIOD REQUESTED'
        continue
            
    # drop timestamp from Netezza
    dateDf['DATE'] = dateDf[dateDf.columns[0]].dt.date
                                  
    # sort the values by date
    dateDf_sorted = dateDf.sort_values('DATE')
    
    # calculate the average time difference between dates
    time_diff_mean = dateDf_sorted['DATE'].diff().mean()

    # daily is anything less than 3 days
    if time_diff_mean <= timedelta(days = 3):
        df.loc[df['TABLE_NAME'] == table, 'FREQ'] = 'daily'

    # weekly is anything within 5 days to 8 days
    elif (time_diff_mean > timedelta(days=5)) & (time_diff_mean <= timedelta(days=8)):
        df.loc[df['TABLE_NAME'] == table, 'FREQ'] = 'weekly'

    # monthly is everything that is within 25 days to 32 days
    elif (time_diff_mean > timedelta(days=25)) & (time_diff_mean <= timedelta(days=32)):
        df.loc[df['TABLE_NAME'] == table, 'FREQ'] = 'monthly'

    # quarterly between a 89 to 95 days
    elif (time_diff_mean > timedelta(days=88)) & (time_diff_mean <= timedelta(days=95)):
        df.loc[df['TABLE_NAME'] == table, 'FREQ'] = 'quarterly'
    
    # mean time difference of everything that does not fall within these buckets is put in place of frequency
    else:
        df.loc[df['TABLE_NAME'] == table, 'FREQ'] = str(time_diff_mean)
        

# save results to CSV for reference
df.to_csv('table_frequency_{}_ALL.csv'.format(db), index = False)

cursor.close()
conn.close()

end = time.time()

print(end-start)
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  4 11:30:23 2018

@author: hanm1
"""

## Goal: Gather periodic statistics for numerical columns to be referenced for Outlier Checking
#### MUST BE RAN FIRST BEFORE RUNNING FRAMEWORK

import os
import xml.etree.ElementTree as et
import pandas as pd
import numpy as np
from statsmodels import robust
import pyodbc


# Assign working directory
## ADD TO CONFIG 
os.chdir(r'C:\Users\hanm1\Desktop\Framework\Python')
os.getcwd()

# import XML config file
tree = et.parse('CONF.xml')
root = tree.getroot()

# Parse through XML file
uid = root.find('Netezza/uid').text
hostname = root.find('Netezza/hostname').text
port = root.find('Netezza/port').text
database = root.find('Netezza/db').text

# splitting db, tbl, columns, and col_grp for multiples
db = (root.find('DATABASE/DB/db').text)
tbl = (root.find('DATABASE/DB/tbl_name').text).split(',')
columns = (root.find('DATABASE/DB/COLUMN/col').text).split(',')
col_grp = (root.find('DATABASE/DB/COLUMN/col_grp').text).split(',')

# Connection settings for ODBC
conn = pyodbc.connect("DRIVER={NetezzaSQL};SERVER="+hostname+";PORT="+port+";DATABASE="+database+
                      ";UID="+uid+";PWD="+root.find('Netezza/pwd').text+";DSN=NZSQL")

cursor = conn.cursor()



# Empty list/dataframe to store results
data = []
nonCat = []
numCat = []
res = pd.DataFrame()


# within each table loop through the tbl_name
for tbl in tbl:

    # Remove any preceding spaces
    tbl = tbl.strip()
    
    # for each table loop through the columns
    for column in columns:
    
        # Remove any preceding spaces
        column = column.strip()
        
        # beginning time period for statistics from xml file
        begin = root.find('DATABASE/begin').text
        
        # end of time period for statistics from xml file
        end = root.find('DATABASE/end').text

        # sql query to get weekly table headers 
        column_sql = 'select * from {}..{} limit 1'.format(db, tbl)
        headers = pd.read_sql_query(column_sql, conn).columns.tolist()
        
        # filter out date_id column
        date_header_list = [col for col in headers if '_DATE_ID' in col]
        
        # pull date_id column header from netezza
        date_header = date_header_list[0]
        
        # SQL Query
        sql = 'select {0} from {1}..{2} where {3} between {4} and {5}'.format(column, db, tbl,date_header, begin, end)
        
        # Loads SQL table into Python
        df = pd.read_sql_query(sql, conn)

        # All columns to list
        head = df.columns.values.tolist()
        
        # Identify columns with only null values to drop
        null_col = df.loc[:, df.isna().all()].columns.values.tolist()
        
        # remove null only columns from analysis
        headers = list(set(head) - set(null_col))
        
        # Force ID columns to be string
        IDcols = [col for col in df.columns if '_ID' in col]
        df[IDcols] = df[IDcols].astype('str')

        # Identify date columns as date data types
        dHeaders = list(set(headers) - set(IDcols))
        dateCols = [col for col in dHeaders if '_DATE' in col or '_DATE_' in col]
        df[dateCols] = df[dateCols].astype('datetime64[ns]')

        # Identify Numeric Columns
        numdf = df.select_dtypes(include = np.number)
        numHead = numdf.columns.values.tolist()
        numHeaders = list(set(numHead) - set(IDcols) - set(dateCols))
        
        
        #######################
        ### Numeric Columns ###
        #######################
        
        ## Loop to get count of unique values and monthly stats if they're ungrouped grouped
        for n in numHeaders:
            numCol = df[n].dropna()
            # separate numerical columns vs categorical variables
            if numCol.nunique() > 40:
                nonCat.append(numCol.name)
            # categories that have less than 40 unique values
            elif numCol.nunique() <= 40:
                numCat.append(n)
       
        ## Calculate Monthly stats if no group was assigned ##
        if col_grp not in headers:
            for num in nonCat:
                col = df[num].dropna()
                mean = col.mean()
                stddev = col.std()
                # Store periodic statisics into a dataframe
                names = ['mean', 'stddev']
                values = [mean, stddev]
                data.append(dict(zip(names, values)))
        
            # Statistics when col_grp is blank
            ungroupedStats = pd.DataFrame(data, nonCat)
        
        ## End of monthly ungrouped stats ##
        
        # Determines whether the col_grp applies to the current table and groups the mean/std by col_grp
        elif col_grp in headers:
            # Grouped statistics based on col_grp XML value
            groupedHead = nonCat + col_grp
            grouped = df[groupedHead].groupby(col_grp)
            groupedStats = grouped.agg([np.mean, np.std, robust.mad, np.median, len])
        
        # monthly stats to csv file print message
        print('Monthly Stats for {}..{} have been saved here:\n'.format(db, tbl), os.getcwd())
        
        # CSV file saved based on grouped or ungrouped stats
        ## ungroupedStats columns will always have 1 column (count)
        if len(ungroupedStats.columns) > 1:
            ungroupedStats.to_csv('ungroupedMonthlyStats_{0}_{1}.csv'.format(db, tbl))
        else:
            # grouped stats based on col_grp from XML to csv
            groupedStats.to_csv('groupedMonthlyStats_{0}_{1}.csv'.format(db, tbl))
        
        #############################
        ### Categorical Columns ###
        #############################

        # Empty Dataframe to store results
        emp2 = pd.DataFrame()
        
        # Frequency of categories on monthly
        strDf = df.select_dtypes(include = object)
        strCols = strDf.columns.tolist()
        strHeaders = list(set(strCols) - set(IDcols) - set(null_col))
        
        # empty lists to store results
        catStr = []
        otherStr = []
        for i in strHeaders:
            # taking number of unique string values to determine categorical
            if df[i].nunique() < 40:
                catStr.append(df[i].name)
            else:
                otherStr.append(df[i].name)
        
        # Strip white spaces for analysis
        df[catStr] = df[catStr].apply(lambda x: x.str.strip()).replace('', 'None')
        
        # combine categorical string and numerical categorical
        category = catStr + numCat
        
        # Collect average frequency and standard deviations based on dates
        for s in category:
            # Average
            avgFreq = df.groupby(['{}'.format(date_header).strip(), s]).size().groupby(level=1).mean().rename('{}_meanFreq'.format(s))
            # Standard Deviation
            stdFreq = df.groupby(['{}'.format(date_header).strip(), s]).size().groupby(level=1).std().rename('{}_stdFreq'.format(s))
            
            # temp dataframe to combine average and standard deviation of frequencies
            temp = pd.concat([avgFreq, stdFreq], axis = 1, sort = False)
            
            # store results in a dataframe
            emp2 = emp2.append(temp)
        
        print('Categorical Frequency Statistics for {0}..{1} stored:\n'.format(db,tbl), os.getcwd())
        emp2.to_csv('Categorical_Freq_{0}_{1}.csv'.format(db, tbl))
            
# should force the loop to skip to the next database if the table is not within it
#except DatabaseError:
#    continue

print('\n SCRIPT COMPLETE \n ')
# Close connection
cursor.close()
conn.close()



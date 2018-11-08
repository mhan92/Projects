# -*- coding: utf-8 -*-
"""
Created on Wed May 30 11:19:40 2018

@author: hanm1
"""

import os
import sys
import xml.etree.ElementTree as et
import pandas as pd
import numpy as np
import scipy.stats as stats
from datetime import date, timedelta
import pyodbc

os.chdir(r'C:\Users\hanm1\Desktop\Framework\Python')
#os.chdir(workDir)
wd = os.getcwd()

# Import Column Outlier Check
import column_check


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

# Assign working directory ### Add to config file

# Connection settings for ODBC
conn = pyodbc.connect("DRIVER={NetezzaSQL};SERVER="+hostname+";PORT="+port+";DATABASE="+database+
                      ";UID="+uid+";PWD="+root.find('Netezza/pwd').text+";DSN=NZSQL")
cursor = conn.cursor()

   
# within each table loop through the tbl_name
for tbl in tbl:
    
    # Remove any preceding spaces
    tbl = tbl.strip()
    
    # for each table loop through the columns
    for column in columns:
        
        # Remove any preceding spaces
        column = column.strip()
        
        # perform outlier analysis over the current date minus 1
        today = date.today() - timedelta(days = 1)
        
        # reformat date for sql query
        today = today.strftime('%Y%m%d') 
        
        today = 20180720
        
        # sql query to get weekly table headers 
        column_sql = 'select * from {}..{} limit 1'.format(db, tbl)
        headers = pd.read_sql_query(column_sql, conn).columns.tolist()
        
        # filter out date_id column
        date_header_list = [col for col in headers if '_DATE_ID' in col]
        
        # pull date_id column header from netezza
        date_header = date_header_list[0]
        
        # SQL Query
        sql = 'select {0} from {1}..{2} where {3} = {4} limit 100000'.format(column, db, tbl, date_header, today)

        # Loads SQL table into Python 
        df = pd.read_sql(sql, conn)

        #####################
        ### Cleaning Data ###
        #####################

        # Identify Columns with only null values
        null_col = df.loc[:, df.isna().all()].columns.values.tolist()
        if len(null_col) != 0:
            print('These columns only contain null values:\n', null_col)
        elif len(null_col) == len(df.columns):
            print('{}..{} is completely empty for {}'.format(db, tbl, today))
        else:
            pass

        # Give count of total Null values in each column
        totalNull = df.isnull().sum()
        columnTotalNull = totalNull[totalNull!=0].rename('Total Number of Null Values')
        if len(columnTotalNull) != 0:
            print('Count of Null Values in each Column:\n',columnTotalNull)
        elif len(columnTotalNull) == 0:
            pass
 
        # List for all columns headers EXCLUDING null only columns
        col_names = df.columns.values.tolist()
        headers = list(set(col_names) - set(null_col))

        # Force ID columns to be string
        IDcols = [col for col in df.columns if '_ID' in col]
        df[IDcols] = df[IDcols].astype('str')
        
        # Identify date columns as date data types
        dHeaders = list(set(headers) - set(IDcols))
        dateCols = [col for col in dHeaders if '_DATE' in col or '_DATE_' in col or '_DT' in col]
        df[dateCols] = df[dateCols].astype('datetime64[ns]')

        # Identify Numeric Columns
        numdf = df.select_dtypes(include = np.number)
        numHeaders = numdf.columns.values.tolist()

        # Idenify String Columns
        strdf = df.select_dtypes(include = object)
        
        # Removing ID and date columns from string headers
        strHeaders = list(set(headers) - set(IDcols) - set(dateCols) - set(numHeaders))
        
        # differentiate between free text and categorical text
        catStr = []
        otherStr = []
        
        for i in strHeaders:
            # categorical variables are classified as less than 100 unique values
            if df[i].nunique() < 40:
                catStr.append(df[i].name)
            else:
                otherStr.append(df[i].name)
        
        # strip white spaces for categorical variables
        df[catStr] = df[catStr].apply(lambda x: x.str.strip()).replace('', 'None')
        
        ##############################################
        ### Outlier Check for Numeric column value ###
        ##############################################

        # Empty dataframes to store results
        emp1 = pd.DataFrame()
        ser = pd.Series()
        num = []
        
        ## Load Reference Summary Stats for grouped
        if os.path.exists('groupedMonthlyStats_{0}_{1}.csv'.format(db, tbl)):
            statRef = pd.read_csv('groupedMonthlyStats_{0}_{1}.csv'.format(db, tbl), index_col = 0, header = [0,1])
            
            # pull columns that are on stats csv for numeric variables
            statsHeaders = stats.columns.tolist()
            
            # iterate through column headers to identify which columns are non-categorical
            for s in statsHeaders:
                addTo = [col for col in numHeaders if s[0] in col]
                num.append(addTo)
                
            # convert list to series to pull unique values
            num = pd.Series( (v[0] for v in num) )
            
            # final num list with each column that is not categorical
            num = num.unique().tolist()
            
            ## Numeric Columns
            for n in num:
                
                # reference stats for column by group
                tmp = stats[[n]]
                
                # remove groups with one observation or stddev = 0
                tmp = tmp.loc[(tmp[n]['len'] > 1) | (tmp[n]['std'] > 0)]
                
                # loop through index within numeric column groups
                for idx in tmp.index:
                    
                    # Load in reference stats
                    refMean = tmp.loc[idx, tmp.columns[0]]
                    refStd = tmp.loc[idx, tmp.columns[1]]
                    
                    for item in col_grp:
                        # subset dataframe for rows with col_grp
                        subset = df.loc[df[item] == idx]
                        continue
                    
                    # Locate column of interest
                    ncol = subset[n].dropna()
                    
                    # Checks whether the column is normal distributed and uses the mean and standard deviation
                    if stats.normaltest(ncol)[1] > 0.05:
                        chk = column_check.num_check(ncol, refMean, refStd)
                    
                    # otherwise, the column_check script will calculate the median and the absolute median deviation and determine outliers
                    else:
                        chk = column_check.mad_check(ncol)
                    
                    # Appends outliers to empty dataframe
                    ser = ser.append(chk).rename(n)
                    
                    # store outliers in an empty dataframe
                emp1 = emp1.append(ser, verify_integrity = True)
                
                # reset series for loop
                ser = pd.Series()

        # load in reference stats for ungrouped
        elif os.path.exists('ungroupedMonthlyStats_{0}_{1}.csv'.format(db, tbl)):
            statRef = pd.read_csv('ungroupedMonthlyStats_{0}_{1}.csv'.format(db, tbl),index_col = 0)
            
            # pull columns that are on stats csv for numeric variables
            ## based on index because the statistics are not grouped
            statsHeaders = statRef.index.tolist()
            
            # iterate through column headers to identify which columns are non-categorical
            for s in statsHeaders:
                addTo = [col for col in numHeaders if s in col]
                num.append(addTo)
                
            # convert list to series to pull unique values
            num = pd.Series( (v[0] for v in num) )
            
            # final num list with each column that is not categorical
            num = num.unique().tolist()
            
            for n in num:
                ncol = df[n]
                
                # ignoring null values
                ncol = ncol.dropna()
                
                refMean = statRef.loc[ncol.name, 'mean']
                refStd = statRef.loc[ncol.name, 'stddev']
                
                # Checks whether the column is normal distributed and uses the mean and standard deviation
                if stats.normaltest(ncol)[1] > 0.05:
                    chk = column_check.num_check(ncol, refMean, refStd)
                    
                 # otherwise, the column_check script will calculate the median and the absolute median deviation and determine outliers
                else:
                    chk = column_check.mad_check(ncol)
          
                # Appends outliers to empty dataframe
                emp1 = emp1.append(chk)
                
            # Transpose results for readability
            numOutliers = emp1.transpose() 

        else:    
            # error if no files are found for reference
            sys.exit('Reference stats csv was not saved. Please run monthlyStatistics.py script')

        # Tranpose results for readility
        out = emp1.transpose()
        
        # inserting blank column to break numeric outlier columns from other variables
        out.insert(loc = 0,value = '',column = 'Numeric Column Outliers ->', allow_duplicates = True)
        
        # categorical numeric columns are columns that are not in the stats table
        numCat = list(set(numHeaders) - set(num))
        
        # combine all columns except for numerical to quickly identify outliers
        tmp = strHeaders + numCat
        
        # joining table for outlier analysis
        numOutliers = df[tmp].join(out, how = 'right')
        
        # Changes the file name based on whether a group was specified
        if os.path.exists('groupedMonthlyStats_{0}_{1}.csv'.format(db, tbl)) and (len(numOutliers) != 0):
            
            # Print numeric outliers
            print('\n {0}..{1} rows with Numeric Outliers grouped by {2}:\n'.format(db, tbl, col_grp), numOutliers.dropna(how = 'all'))
            
            # save outliers to csv
            numOutliers.to_csv('{0}_{1}_Numeric_Outliers GROUPED BY {2}.csv'.format(db, tbl, col_grp), index = False)
            print('\n end of numeric outliers \n')
        
        elif os.path.exists('ungroupedMonthlyStats_{0}_{1}.csv'.format(db, tbl)) and (len(numOutliers) != 0):
            # print numeric outliers for ungrouped
            print('\n{1} rows with Numeric Outliers for {0}..{1}\n'.format(db, tbl), numOutliers.dropna(how = 'all'))
            numOutliers.to_csv('{0}_{1}_Numeric_Outliers.csv'.format(db, tbl), index = False)
            print('\n end of numeric outliers\n')
        
        # print message for no numeric outliers
        elif len(numOutliers) == 0:
            print('\n No numeric Outliers for {}..{} on {} \n'.format(db, tbl, today))
    
        ##################################
        ### Categorical Variance Check ###
        ##################################
        
        # load in categorical monthly stats
        refCat = pd.read_csv('Categorical_Freq_{0}_{1}.csv'.format(db, tbl), index_col = 0)
        
        # empty dataframe for results
        emp2 = pd.DataFrame()

        # empty dataframe to store categorical variables results
        newCat = pd.DataFrame()
        categoricalOutliers = pd.DataFrame()
        
        # force categorical numerical values to string
        df[numCat] = df[numCat].astype('str')
        
        # combine categorical string and numerical columns together
        categoricalCols = catStr + numCat
        
        # replace null values in string with ''
        mask = df.applymap(lambda x: x is None)
        cols = df.columns[(mask).any()]
        for cols in df[cols]:
            df.loc[mask[cols], cols] = 'None'
        
        # iterate through each categorical variables - defined to be any column with <= 40 unique values
        for category in categoricalCols:
           
            # group dataframe by date and category to give frequency by day
            cnt = df.groupby([category]).size().reset_index()
            
            # pulls stats from reference csv
            stats_table = refCat[['{}_meanFreq'.format(category), '{}_stdFreq'.format(category)]].dropna(how = 'all')
            
            # loop through grouped dataframe and pull dates with more/less than expected
            for idx in cnt[category]:
                
                # select row from count dataframe and rename 0 column to be count
                df_select = cnt.loc[cnt[category] == idx].rename(columns = {0: 'cnt'})
                
                # force numeric values to be string for categorical variables
                idx = str(idx)
                
                # checks if the category is in the reference stats index
                if idx in stats_table.index:
                    
                    # pull reference statistics from the reference category table
                    refMean = stats_table.loc[stats_table.index == idx]['{}_meanFreq'.format(category)][0]
                    refStd = stats_table.loc[stats_table.index == idx]['{}_stdFreq'.format(category)][0]
                    
                    # ignores categories with 0 or null std deviation
                    if refStd != 0:
                        
                        # added [0] to pull the actual values with df_select
                        if (df_select['cnt'].iloc[0] > refMean + 2 * refStd) or (df_select['cnt'].iloc[0] < refMean - 2 * refStd):
                            
                            # input average to table
                            df_select['MEAN'] = refMean
                            df_select['STDDEV'] = refStd
                            
                            # output outliers if the date is more or less then expected
                            categoricalOutliers = categoricalOutliers.append(df_select, sort = False)
                    elif refStd == 0:
                        df_select
                   
                elif idx not in stats_table.index:
                    # places new categories in a separate dataframe 
                    newCat = newCat.append(df_select, sort = False)
            
        # prints output if there is output to print
        if len(categoricalOutliers) != 0:

            # rearrange columns so mean and cnt are at the front of the dataframe
            
            # select mean and cnt cols
            meanCols = categoricalOutliers['MEAN']
            stddevCols = categoricalOutliers['STDDEV']
            cntCols = categoricalOutliers['cnt']
            
            # droppping mean and cnt cols
            categoricalOutliers.drop(labels=['MEAN'], axis=1,inplace = True)
            categoricalOutliers.drop(labels=['STDDEV'], axis=1,inplace = True)
            categoricalOutliers.drop(labels=['cnt'], axis=1,inplace = True)
            
            # reinserting cols for readability
            categoricalOutliers.insert(0, 'CURR_COUNT', cntCols)
            categoricalOutliers.insert(1, 'MEAN', meanCols)
            categoricalOutliers.insert(2, 'STDDEV', stddevCols)
            
            print('\n Categorical Outliers for {}..{}\n'.format(db,tbl))
            print(categoricalOutliers)
            categoricalOutliers.to_csv('{}..{}_categorical_outliers.csv'.format(db,tbl))
            
        if len(newCat) != 0:
            # regorganize columns by putting cnt in the beginning of the dataframe
            newCntCols = newCat['cnt']
            newCat.drop(labels=['cnt'], axis=1,inplace = True)
            newCat.insert(0, 'cnt', newCntCols)
            newCat.reset_index(drop = True)
            
            print('\n New Categories for {}..{}\n'.format(db, tbl))
            print(newCat)
            newCat.to_csv('{}..{}_new_categories.csv'.format(db, tbl))

            
        elif len(categoricalOutliers) == 0:
            print('\n No categorical Outliers for {}..{} on {} \n'.format(db, tbl, today))
        elif len(newCat) == 0:
            print('\n No new Categories for {}..{} on {} \n'.format(db, tbl, today))


print('\n SCRIPT FINISHED \n')
# Closing connection
cursor.close()
conn.close()

# -*- coding: utf-8 -*-
"""
Created on Fri May 31 15:22:59 2019

@author: mhan
"""


'''
This function compares the df and ref_df together using the column 'STATUS'
I have joined the dataframes together on a portion of the unique identifier -- the location and time portion
the function iterates through each row and performs the if statements below to identify if the item is an update, new, or 'OKAY' (meaning an old item)
'''

def dupCheck(df, ref_df):
    import pandas as pd
    
    pd.options.mode.chained_assignment = None # used to remove the settingwithcopwarning from pandas
    
    # Get the time and location of the play from the unique ID
    df['time_id'] = pd.to_datetime(df['ID'].str.split(":",expand = True)[0], format = "%H%M").dt.time
    df['location_id'] = df['ID'].str.split(":",expand = True)[1]
    ref_df['time_id'] =  pd.to_datetime(ref_df['ID'].str.split(":", expand = True)[0], format = "%H%M").dt.time
    ref_df['location_id'] = ref_df['ID'].str.split(":",expand = True)[1]
    ref_df['Status'] = 'OKAY' ## resets the playlist reference status to be OKAY to adjust for the if statement comparison
    
    # Merged the csv reference file with the new playlist and merged on location & time
    merged = pd.merge(df, ref_df, how = 'outer', on = ['location_id', 'time_id']).sort_values(by = 'time_id').reset_index(drop = True)
    
    # iterates through each row for classification
    for i in range(len(merged)):
        if merged['Status_y'][i] == 'OKAY' and merged['Status_x'][i] == 'OKAY': ## finding dups if the status of the new and old dataframe are both okay
            # if statement that compares the content of the engagement activity and ensures that the content is the same
            if merged['EngagementActivity/Notes_x'][i] == merged['EngagementActivity/Notes_y'][i]: 
                merged['Status_x'][i] = 'DUP' 
            elif merged['EngagementActivity/Notes_x'][i] != merged['EngagementActivity/Notes_y'][i]:
                # keeps track of the unique identifers so that the updated playlist items retain the original unique id
                update_id = {'old': merged['ID_x'][i], 'new': merged['ID_y'][i]}
                # using the 'old' unique id, change status to update for the correct playlist items
                merged.loc[merged['ID_x'] == update_id['old'], 'Status_x'] = 'UPDATE'
                merged.iloc[i]['ID_x'] = update_id['new']
        
        # blanket statement for new items: if it does not exist in the reference playlist and is in the new playlist then NEW
        ## the if statement takes into account if the function already classified rows as updates
        elif merged['Status_y'][i] != 'OKAY' and (merged['Status_x'][i] == 'OKAY' or merged['Status_x'][i] != 'UPDATE' or merged['Status_x'][i] != 'DUP'):
            # subset dataframe for the locations that the plays occur at
            locat = merged.iloc[i]['location_id']
            # performs another merged statement only on location_id 
            merged_subset = pd.merge(df, ref_df, how = 'outer', on = 'location_id')
            subset = merged_subset.loc[merged_subset['location_id'] == locat]
            # converts the schedtime to datetime objects so we can perform time difference evaluations
            subset['SchedTime_x'] = pd.to_datetime(subset.SchedTime_x.astype('str'))
            subset['SchedTime_y'] = pd.to_datetime(subset.SchedTime_y.astype('str'))
            subset['Hour_x'] = subset['SchedTime_x'].dt.hour
            subset['Hour_y'] = subset['SchedTime_y'].dt.hour
            
            leftover = subset.loc[subset.Hour_x == subset.Hour_y]
            if len(leftover) != 0:
                # replaces the subset object so that any playlist that occur within the hour but not at the same time (same time is captured in the first if statement)
                leftover = leftover[((leftover["SchedTime_x"] - leftover["SchedTime_y"]).abs() < pd.Timedelta("1 hours")) &
                            ((leftover['SchedTime_x'] - leftover['SchedTime_y']).abs() != pd.Timedelta("0 seconds"))]
                # keeps track of the unique identifers so that the updated playlist items retain the original unique id
                update_id = {'old': leftover.ID_x.tolist(), 'new': leftover.ID_y.tolist()}
                # using the 'old' unique id, change status to update for the correct playlist items
                merged.loc[merged['ID_x'].isin(update_id['old']), 'Status_x'] = 'UPDATE'
            else:
                merged['Status_x'][i] = 'NEW' 

        ## finding updated playlist items
        elif merged['Status_y'][i] == 'OKAY' and (merged['Status_x'][i] != 'OKAY' or merged['Status_x'][i] != 'UPDATE' or merged['Status_x'][i] != 'DUP'):

            # subset dataframe for the locations that the plays occur at
            locat = merged.iloc[i]['location_id']
            # performs another merged statement only on location_id 
            merged_subset = pd.merge(df, ref_df, how = 'outer', on = 'location_id')
            subset = merged_subset.loc[merged_subset['location_id'] == locat]
            # converts the schedtime to datetime objects so we can perform time difference evaluations
            subset['SchedTime_x'] = pd.to_datetime(subset.SchedTime_x.astype('str'))
            subset['SchedTime_y'] = pd.to_datetime(subset.SchedTime_y.astype('str'))
            subset['Hour_x'] = subset['SchedTime_x'].dt.hour
            subset['Hour_y'] = subset['SchedTime_y'].dt.hour
            
            leftover = subset.loc[subset.Hour_x == subset.Hour_y]
            # replaces the subset object so that any playlist that occur within the hour but not at the same time (same time is captured in the first if statement)
            leftover = leftover[((leftover["SchedTime_x"] - leftover["SchedTime_y"]).abs() < pd.Timedelta("1 hours")) &
                            ((leftover['SchedTime_x'] - leftover['SchedTime_y']).abs() != pd.Timedelta("0 seconds"))]
            # keeps track of the unique identifers so that the updated playlist items retain the original unique id
            update_id = {'old': leftover.ID_x.tolist(), 'new': leftover.ID_y.tolist()}
            # using the 'old' unique id, change status to update for the correct playlist items
            merged.loc[merged['ID_x'].isin(update_id['old']), 'Status_x'] = 'UPDATE'
            
            # now we replace the 'new' unique ids created from the new playlist with the reference playlist
            ## we remove the extra columns when we concatenate and clean the datset in the actual program
            merged = merged.replace(update_id['old'], update_id['new'])
        else:
            merged['Status_x'][i] = 'DELETED' ## everything else should be a deleted item ---- could require more investigation
            
    # Takes new and updated plays from the total dataframe
    new_plays = merged.loc[merged['Status_x'].isin(['NEW', 'UPDATE', 'DELETED'])].dropna(axis = 1, how = 'all')
    
    # drops the suffix from the merge
    new_plays.columns = new_plays.columns.str.rstrip('_x')
    # returns the remaing dataframe in the correct column order
    return new_plays[['ID', 'Date', 'SchedTime', 'EngagementActivity/Notes', 'WorkGroup', 'Terminal', 'Status']]
    

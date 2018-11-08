# -*- coding: utf-8 -*-
"""
Created on Thu May 24 08:04:47 2018

@author: HanM1
"""

### Numeric Outlier Check ###

# Goal: Identify Outliers for Numeric columns using reference stats 
def num_check(col, refMean, refStd):
    # calculate outlier bounds
    up = refMean + 2 * refStd
    low = refMean - 2 * refStd
    out = col[(col > up) | (col < low)]
    return out

## Median Absolute Deviation
# Robust Outlier Detection Model
# Goal: detect outliers using MAD method 'more robust than standard deviation/mean'
def mad_check(col):
    import numpy as np
    from statsmodels import robust
    med = np.median(col)
    mad = robust.mad(col)
    # Calculate outlier bounds
    ## cut off of 3 or 3.5 is suggested
    up = med + 3 * mad
    low = med - 3 * mad
    out = col[(col > up) | (col < low)]
    return out

## IQR Outlier Detection
# Goal: Outliers classified as being 1.5 of the Inner Quartile Range
# Less robust than Median Absolute Deviation but more robust than stddev and mean approach
def iqr_check(col):
    import numpy as np
    quartile_1, quartile_3 = np.percentile(col, [25, 75])
    iqr = quartile_3 - quartile_1
    low = quartile_1 - (iqr * 1.5)
    up = quartile_3 + (iqr * 1.5)
    out = col[(col > up) | (col < low)]
    return out



# Attribute Value Frequnecy
## Get the frequency an attribute values occurs at each column, the sum the frequencies across the row 
## divided by the total number of attributes == Attribute Value Score.
## rows with the lowest scores are considered to be 'OUTLIERS'
def avf_check(df, categoricalCols):
    import numpy as np
    import pandas as pd
    # empty dataframe to store results
    emp2 = pd.DataFrame()
    # identify unique values
    array = df[categoricalCols].values
    # create frequency matrix of distinct values
    freq_matrix = np.unique(array, return_counts = True)
    # separating out arrays to create dictionary
    var = freq_matrix[0].tolist()
    freq = freq_matrix[1].tolist()
    freq_dict = dict(zip(var, freq))
    # calculate Attribute Value Frequency Scores
    for x in categoricalCols:
        n = df[x]
        mapped = n.map(freq_dict)
        emp2 = emp2.append(mapped)
    avf_matrix = emp2.transpose()
    avf_matrix['score'] = avf_matrix.sum(axis = 1)/len(categoricalCols)
    # Calculate score average and standard deviation
    avg_score = avf_matrix['score'].mean()
    stddev_score = avf_matrix['score'].std()
    # rows with the lowest scores are outliers
    low = avg_score - 3 * stddev_score
    # identify outliers
    return df[(avf_matrix['score'] < low)]



    
# Crude outlier check, based on frequency threshold of 10%. 
def categoric_check(col):   
    # Frequency of each category
    count = col.value_counts()    
    countHeader = col.columns    
    # Total observations in dataframe
    total = len(col)    
    # Average frequency
    freq = count/total    
    # Identify Outliers with frequency less than 10%
    ## Index resetting and assigning column names
    categorical_outlier = freq.loc[freq['freq'] < .10].reset_index()    
    # dropping frequency column
    selected = categorical_outlier.rename(columns={'index':countHeader}).drop(['freq'], axis = 1)    
    return selected
    

# Chebyshev Outlier Check Function
### Numeric Outlier Check ###
# Goal: Identify outliers using Chebyshev's Theorem. #
# Calculate interval where acceptable data lies using mean and stddev 

def cheb_check(col):
    ### probability to determine which data are potential numeric outliers
    # Probability to determine which data are potential numeric outliers
    
    ## 80% of data is okay
    p1 = .20
    
    ### expected probability of seeing an numeric outlier
    # Probability of seeing a numeric outlier
    ## 10% of all data will be an outlier, other 10% could be noise in the dataset
    p2 = .10
    
    # Store column stats
    avg = col.mean()
    stdev = col.std()  
    
    # Calculate bounds
    up1 = avg + (1 / (p1 ** (.5))) * stdev
    low1 = avg - (1 / (p1 ** (.5))) * stdev   
    
    # Filter on condition
    nonOut = col[(col < up1) & (col > low1)]
    
    # Calculate new stats
    avg2 = nonOut.mean()
    stdev2 = nonOut.std()
    
    # Calculate True Outlier Bounds
    up2 = avg2 + (1 / (p2 ** (.5))) * stdev2
    low2 = avg2 - (1 / (p2 ** (.5))) * stdev2
    
    # Re-identify outliers
    out = col[(col > up2) | (col < low2)]
    return out

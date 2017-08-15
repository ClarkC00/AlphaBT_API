# -*- coding: utf-8 -*-
"""
Created on Tue Aug 15 10:44:39 2017

@author: chenxi
"""
import os
from sas7bdat import SAS7BDAT
import numpy as np
import pandas as pd

def stockNameTrans(stockNum):
    
    stockNumStr = str(stockNum).zfill(6);
    
    if stockNumStr > '100000':
        stockCode = stockNumStr + '.SH'
    else :
        stockCode = stockNumStr + '.SZ'
        
    return stockCode

infile = r'F:\Quant\AlphaBT\DataBase3\Future\Future_SAS_files\shsz300_idx_dwt.sas7bdat'

outputDir = r'F:\Quant\AlphaBT\DataBase3\Future'
outfileName = 'if300'

start_date = '2011-01-04'
end_date = '2017-08-10'

with SAS7BDAT(infile) as f:
    df = f.to_data_frame()
    
df['enddt'] = pd.to_datetime(df['enddt'])
df['stkcd'] = df['stkcd'].map(int).map(stockNameTrans)

df=df.pivot_table(index='enddt',columns='stkcd',values='standard_wt', aggfunc=np.mean) # for duplicate apply mean function

#df1 = df.set_index(['enddt','stkcd'])

#df2 = df['standard_wt'].unstack()




validFile = r'F:\Quant\AlphaBT\DataBase3\basedata\TOP50.csv'
valid = pd.read_csv(validFile, index_col = 0)
valid[:] = np.nan

futureValid = pd.DataFrame(data = [], index = valid.T.index)

futureValid = futureValid.join(df.T, how = 'outer')

futureValid = futureValid.T

futureValid = futureValid[start_date : end_date]

futureWeights = futureValid.copy()

futureValid[np.isnan(futureValid)] = 0
futureValid[futureValid > 0] = 1


fileName = os.path.join(outputDir, outfileName+'.csv')
futureValid.to_csv(fileName, header = True)

fileName = os.path.join(outputDir, outfileName + '_weights' + '.csv')
futureWeights.to_csv(fileName, header = True)


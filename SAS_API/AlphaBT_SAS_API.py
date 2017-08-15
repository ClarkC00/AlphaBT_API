# -*- coding: utf-8 -*-
"""
Created on Tue Aug 15 10:44:39 2017

@author: chenxi
"""

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

infile = r'F:\Quant\AlphaBT\DataBase3\Future\Future_SAS_files\csi500_idx_dwt.sas7bdat'


with SAS7BDAT(infile) as f:
    df = f.to_data_frame()
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

futureWeights = np.copy(futureValid)

futureValid[futureValid > 0] = 1


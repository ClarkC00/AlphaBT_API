# -*- coding: utf-8 -*-
"""
Created on Wed May 24 10:23:52 2017

@author: chen
"""
import numpy as np
import pandas as pd
import pymysql
import os
#==============================================================================
# get price and value data from Data Base
#==============================================================================

def getPVDataFromDB():
    
    mysql_cn = pymysql.connect(host='quant-station2',
                   port=3306, user='dbAdmin', password='dbQuant',
                   db='price_factor')
    print('Fetch price and vol data to Local....')
    query = '''
    SELECT id, security_id, tradeday, open, high, low, close, volume, amount,
        turn, adjfactor
    		  FROM stock_day_data
    		  '''
    
    df_mysql = pd.read_sql(query,con=mysql_cn)
    
#    df_mysql.to_csv(outFilePath)
    
    print('Fetch price and vol data to local sucessfully!')
    
    return df_mysql


#==============================================================================
# make data format like websim
#==============================================================================

def updatePVData(data_df, time_index = 'tradeday', code_index = 'security_id'):
    # data_df is like a list of items with time, stock code, and data
    
    
    
    data_new_df = data_df
    
    fileNameList = ['close','open','high','low','volume','tvrvalue']
    varNameList = ['close','open','high','low','volume','amount']

    outputFileDirPath = r'D:\Quant\DataBase2\basedata'         
#    data_old_df = pd.read_csv(filePath, index_col = 0)
    
    for i in range(len(varNameList)):
 
        
        iVarName = varNameList[i]
        print('updating ' + iVarName + ' .....')
        var_new_df = data_new_df.set_index([time_index,code_index])[iVarName].unstack()
        adjf_new_df = data_new_df.set_index([time_index,code_index])['adjfactor'].unstack()
        
        if iVarName != 'volume':
            
            var_new_df = var_new_df*(adjf_new_df / np.array(adjf_new_df.max(axis = 0)))
        
        else:
            
            pass       
        
        iFileName = fileNameList[i]                        
        filePath = os.path.join(outputFileDirPath,iFileName+'.csv') 
        var_new_df.to_csv(filePath)
        
        if iVarName == 'close':
            print('updating return using close ....')
            return_df = var_new_df/var_new_df.shift(1) - 1
            filePath = os.path.join(outputFileDirPath,'return.csv') 
            return_df.to_csv(filePath)
        else:
            pass
        
    return
        
#==============================================================================
# update valid files
#==============================================================================
def updateValidData():
    
    print('updating valid ....')
    
    volFileDirPath = r'D:\Quant\DataBase2\basedata\volume.csv'
    volume = pd.read_csv(volFileDirPath, index_col = 0)
    volume[volume <=0] = np.nan
          
    windowSize = 90
    universe = 1000
    
    volume_mv = volume.rolling(window=windowSize,center=False).mean()
    
    volume_mv.iloc[0:windowSize -1,:] = volume.iloc[0:windowSize -1,:]
    
    volume_rank = volume_mv.rank(axis = 1)
    
    valid = volume_rank.copy()

    ind = (volume_rank <= universe)
    valid[:] = 0
    valid[ind] = 1
    
    validFileDirPath = r'D:\Quant\DataBase2\basedata\TOP1000.csv'
    valid.to_csv(validFileDirPath)
    
    return

#==============================================================================
# get industry files from Data base
#==============================================================================
def getIndustryFilesFromDB():
    
    mysql_cn = pymysql.connect(host='quant-station2',
                   port=3306, user='dbAdmin', password='dbQuant',
                   db='industry_factor', charset = 'utf8')
    
    print('Fetch industry data to Local....')
    query = '''
    SELECT security_id, tradeday, industry_citic
    		  FROM industry_citic
    		  '''
    
    df_mysql = pd.read_sql(query,con=mysql_cn)
    
#    df_mysql.to_csv(outFilePath)
    
    print('Fetch industry data to local sucessfully!')
    
    return df_mysql


#==============================================================================
# update industry file format to data frame
#==============================================================================

def updateIndustry(data_df, time_index = 'tradeday', code_index = 'security_id'):
    
    iVarName = 'industry_citic'
    var_new_df = data_df.set_index([time_index,code_index])[iVarName].unstack()
    
    return var_new_df

#==============================================================================
# main manuscript
#==============================================================================

if __name__ =='__main__':
    
#    data_df = getPVDataFromDB()
#    updatePVData(data_df, time_index = 'tradeday', code_index = 'security_id')
    updateValidData()
#    industryData_df = getIndustryFilesFromDB()testtestsd
#sdf
	#
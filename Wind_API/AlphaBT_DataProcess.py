# -*- coding: utf-8 -*-
"""
Created on Mon Aug 14 10:22:00 2017

@author: chenxi
"""
import os
import pandas as pd
import datetime,time
import numpy as np

class DataProcess_Wind(object):
    

    def __init__(self):
        # change this path
        self.InputDir = r'F:\Quant\AlphaBT\TempData\WindData_bfq'
        self.OutputDir = r'F:\Quant\AlphaBT\TempData'
        
        
    def readWindCsv(self, name):
        
        def trimStr(data):
            return data[0:10]
        
        filePath = os.path.join(self.InputDir, name + '.csv')
        
        df = pd.read_csv(filePath, index_col = 0)
        
        df.index = df.index.map(trimStr)
        
        return df
    
    def saveToCsv(self, df, name):
        
        filePath = os.path.join(self.OutputDir, name + '.csv')
        
        df.to_csv(filePath, header = True)
    
    
    def generateValid(self):
        
        def calValid(volume ,windowSize, universe):
            
            volume_mv = volume.rolling(window=windowSize,center=False).mean()
            
            volume_mv.iloc[0:windowSize -1,:] = volume.iloc[0:windowSize -1,:]
            
            volume_rank = volume_mv.rank(axis = 1)
            
            valid = volume_rank.copy()
        
            ind = (volume_rank <= universe)
            valid[:] = 0
            valid[ind] = 1        
            
            ind = volume == 0
            
            valid[ind] = 0
            
            fileName = "TOP" + str(universe)
            self.saveToCsv(valid, fileName)
            
        
        print('updating valid ....')
        
        filePath = os.path.join(self.InputDir, 'volume' + '.csv')
        
        volume = pd.read_csv(filePath, index_col = 0)
        
        universeList = [1500, 1000, 500, 200, 100, 50]        
        windowSize = 90
        
        for universe in universeList:
            print(universe)
            calValid(volume, windowSize, universe)
            
    def DataProcessForWind(self):
        
        adjFactor = self.readWindCsv('adjfactor')
        
        VarList_adj = ["open", "high", "low", "close", "pre_close", "vwap", "amt"]
        
        for varName in VarList_adj:
            print(varName)
            df = self.readWindCsv(varName)
            df = df*(adjFactor / np.array(adjFactor.max(axis = 0)))
            self.saveToCsv(df, varName)
            
            
        VarList = ["dealnum", "volume"]
        
        for varName in VarList:
            print(varName)
            df = self.readWindCsv(varName)
            self.saveToCsv(df, varName)
            
            
        VarList = ['pct_chg']
        for varName in VarList:
            print(varName)
            df = self.readWindCsv(varName)/100
            self.saveToCsv(df, 'returns')
            
            
            
        self.generateValid()
        
    def generateIndustry_temp():
        
        def stockNameTrans(stockNum):
            
            stockNumStr = str(stockNum).zfill(6);
            if stockNumStr > '100000':
                stockCode = stockNumStr + '.SH'
            else :
                stockCode = stockNumStr + '.SZ'
                
            return stockCode
     
        
        
        
#        filePath = os.path.join(self.InputDir, 'sw_industry2' + '.csv')
#        industry_df = pd.read_csv()
#        df = self.readWindCsv('')
#        VarList = ['sw_industry']
#        
#        for varName in VarList:
#            print(varName)
#            self.saveToCsv(df, 'returns')
        
    
if __name__ =='__main__':
    
    temp = DataProcess_Wind()
#    temp.DataProcessForWind()
    opens = temp.readWindCsv('open')
    
    
    industry = pd.read_csv(r'F:\Quant\AlphaBT\TempData\WindData_bfq\sw_industry2.csv')
    industry['code'] = industry['code'].apply(stockNameTrans)
    industry = industry.set_index('code')
    
    opens = opens.index
    
    a = opens.join(industry)
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
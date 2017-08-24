# -*- coding: utf-8 -*-
"""
Created on Thu Aug 24 15:40:28 2017

@author: chenxi
"""
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 14 10:22:00 2017

@author: chenxi
"""
import os
import pandas as pd
import datetime,time
import numpy as np
import matplotlib.pyplot as plt


class DataProcess_Wind(object):
    

    def __init__(self):
        # change this path
        self.InputDir = r'F:\Quant\AlphaBT\TempData\temp'
        self.OutputDir = r'F:\Quant\AlphaBT\TempData\wind_data_20170815'
        
        
    def readWindCsv(self, name, stockCode = None, tradeDay = None):
        
        def trimStr(data):
            return data[0:10]
        
        filePath = os.path.join(self.InputDir, name + '.csv')
        df = pd.read_csv(filePath, index_col = 0)
        df.index = df.index.map(trimStr)
        
        if (stockCode is None) & (tradeDay is None):
            
            print(np.size(df, 1))
        
        else:
            
            df.index = tradeDay['tradeDay']
            df = df.ix[tradeDay['tradeDay'], stockCode['stockCode']]            
            
            print(np.size(df, 1))
        
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
        
        
        filePath = os.path.join(temp.OutputDir, 'stockCode' + '.csv')
        stockCode = pd.read_csv(filePath, index_col = 0)
        
        filePath = os.path.join(temp.OutputDir, 'trade_date_data' + '.csv')
        tradeDay = pd.read_csv(filePath, index_col = 0)
        
        adjFactor = self.readWindCsv('adjfactor', stockCode, tradeDay)
        
        
        # need adjfactor to adjust
        
        VarList_adj = ["open", "high", "low", "close", "pre_close", "vwap", "amt"]
        
        for varName in VarList_adj:
            
            print(varName)
            
            df = self.readWindCsv(varName, stockCode, tradeDay)
            adjFactor.index = df.index
            df = df*(adjFactor / np.array(adjFactor.max(axis = 0)))
            self.saveToCsv(df, varName)
            
            self.checkDataValid(varName)
            
        # not need adjfactors    
        VarList = ["dealnum", "volume","free_turn"]
        
        for varName in VarList:
            
            print(varName)
            
            df = self.readWindCsv(varName, stockCode, tradeDay)
            self.saveToCsv(df, varName)
            self.checkDataValid(varName)
        
        
        # calcualte returns
            
#        VarList = ['pct_chg']
#        for varName in VarList:
#            print(varName)
#            df = self.readWindCsv(varName)/100
#            self.saveToCsv(df, 'returns')   
        
        
        close_df  = self.readWindCsv("close", stockCode, tradeDay)
        
        returns_df = close_df/close_df.shift(1) - 1
        self.saveToCsv(returns_df, 'returns')
        self.checkDataValid('returns')
        
    
    def generateIndustry_temp(self):
        
        
        print('sw  industry')
        def stockNameTrans(stockNum):
            
            stockNumStr = str(stockNum).zfill(6);
            
            if stockNumStr > '100000':
                stockCode = stockNumStr + '.SH'
            else :
                stockCode = stockNumStr + '.SZ'
                
            return stockCode
            
        
        opens = temp.readWindCsv('open')
        tradeDayList = opens.index
        opens = opens.T
        data = pd.DataFrame(data = [], index = opens.index)
        
        
        industry = pd.read_csv(r'F:\Quant\AlphaBT\TempData\WindData_bfq\sw_industry2.csv')
        industry['code'] = industry['code'].apply(stockNameTrans)
        industry = industry.set_index('code')
        
        
        
        for tradeDay in tradeDayList:
#            print(tradeDay)
            industry.columns = [tradeDay]
            data = data.join(industry)
        
        data = data.T
        
        data = data.fillna(-1)
        
        self.saveToCsv(data, 'subindustry')
        self.saveToCsv(data, 'industry_data')
    
    def generateVectorData(self):
        
        opens = self.readWindCsv('close')
        
        stockCode = pd.DataFrame(data = opens.columns, columns = ['stockCode'])
        
        filePath = os.path.join(self.OutputDir, 'stockCode' + '.csv')
        stockCode.to_csv(filePath)
        
        trade_date_data = pd.DataFrame(data = opens.index, columns = ['tradeDay'])
        filePath = os.path.join(self.OutputDir, 'trade_date_data' + '.csv')
        trade_date_data.to_csv(filePath)
        
    def generate_trade_date_data(self):
        
        opens = self.readWindCsv('open')
        
        trade_date_data = pd.DataFrame(data = opens.index, columns = ['tradeDay'])
        filePath = os.path.join(self.OutputDir, 'trade_date_data' + '.csv')
        trade_date_data.to_csv(filePath)
        
        
        
    def checkDataValid(self, fileName):
        
        path = os.path.join(self.OutputDir, fileName + '.csv')
        df = pd.read_csv(path, index_col = 0)
        
        #plot
        plt.figure(1, figsize=(10, 3))
        
        df[df == 0] = np.nan
        
        dataCoverage = df.count(axis = 1)
        
        dataCoverage.plot(grid = True, title = fileName)
        
        plt.show()
        
        return dataCoverage
    
    def generateIndustry(self):
        
        path = os.
    
if __name__ =='__main__':
    
    
    temp = DataProcess_Wind()
    temp.generate_trade_date_data()
    temp.DataProcessForWind()
    
    
    
    temp.generateIndustry_temp()
    
#    opens = temp.readWindCsv('open')
#    tradeDayList = opens.index
#    opens = opens.T
#    data = pd.DataFrame(data = [], index = opens.index)
#    
#    
#    industry = pd.read_csv(r'F:\Quant\AlphaBT\TempData\WindData_bfq\sw_industry2.csv')
#    industry['code'] = industry['code'].apply(stockNameTrans)
#    industry = industry.set_index('code')
#    
#    
#    
#    for tradeDay in tradeDayList:
#        print(tradeDay)
#        industry.columns = [tradeDay]
#        data = data.join(industry)
    
# =============================================================================
#     trans data for single var
# =============================================================================
#    var = temp.readWindCsv('free_turn')
#    filePath = os.path.join(temp.OutputDir, 'stockCode' + '.csv')
#    stockCode = pd.read_csv(filePath, index_col = 0)
#    
#    var = var.ix[:,stockCode['stockCode']]
#    var.to_csv('free_turn.csv', header = True)
    
# =============================================================================
#     checkout single var valid
# =============================================================================
#    temp.checkDataValid('open')
#    temp.checkDataValid('high')
#    temp.checkDataValid('low')
    ss= temp.checkDataValid('volume')
#    
#    
    
    
    
    
    
    
    
    
    
    
    
    
    
    


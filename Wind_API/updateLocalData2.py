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
    
# =============================================================================
#
# =============================================================================
    
    def __init__(self):
        
        # change this path
        self.InputDir = r'F:\Quant\AlphaBT\TempData\temp_download'
        self.OutputDir = r'F:\Quant\AlphaBT\TempData\temp_matrix_data'
        
        filePath = os.path.join(self.InputDir, 'stockCode' + '.csv')
        self.stockCode = pd.read_csv(filePath, index_col = 0)
        
        filePath = os.path.join(self.InputDir, 'tradeDayList' + '.csv')
        self.tradeDay = pd.read_csv(filePath, index_col = 0)
        
        
# =============================================================================
#         
# =============================================================================
        
    def readWindCsv(self, name, stockCode = None, tradeDay = None):
        
        def trimStr(data):
            
            return data[0:10]
        
        filePath = os.path.join(self.InputDir, name + '.csv')
        df = pd.read_csv(filePath, index_col = 0)
        
        print('orginal data shape', np.shape(df))
        
        df.index = df.index.map(trimStr)
        
        if (stockCode is None) & (tradeDay is None):
            
            pass
        
        else:
            
#            df.index = tradeDay['tradeDay']
            df = df.ix[tradeDay['tradeDay'], stockCode['stockCode']]            
            
        print(np.shape(df))
        
        return df
    
# =============================================================================
#     
# =============================================================================
    
    def saveToCsv(self, df, name):
        
        filePath = os.path.join(self.OutputDir, name + '.csv')
        
        df.to_csv(filePath, header = True)
    
# =============================================================================
#     
# =============================================================================
    def generate_valid_matirx(self):
        
        def calValid(var ,windowSize, universe):            
            
            volume = var.copy()
            
            volume = volume.fillna(0)
            
            volume_mv = volume.rolling(window=windowSize,center=False).mean()
            
            volume_mv.iloc[0:windowSize,:] = volume.iloc[0:windowSize,:]
            
            volume_rank = volume_mv.rank(axis = 1,ascending = False)
            
            valid = volume_rank.copy()
        
            ind = ( volume_rank <= universe )
            
            valid[:] = 0
            
            valid[ind] = 1        
            
            
#            stop trading 
            
            print(' this part need to be improved!')
            
            ind = (volume == 0)
            valid[ind] = 0
            
            return valid
        
        print('updating valid ....')
        
        
        filePath = os.path.join(self.OutputDir, 'volume' + '.csv')
        volume = pd.read_csv(filePath, index_col = 0)
        
        universeList = [1500, 1000, 500, 200, 100, 50]        
        
        windowSize = 90
        
        for universe in universeList:
            
            print(universe)
            
            valid = calValid(volume, windowSize, universe)
            
            fileName = "TOP" + str(universe)
            
            self.saveToCsv(valid, fileName)
            self.chenck_data_valid(fileName)
# =============================================================================
#             
# =============================================================================
    def generate_data_matrix(self):
        
        adjFactor = self.readWindCsv('adjfactor', self.stockCode, self.tradeDay)
        
        
        # need adjfactor to adjust
        
        VarList_adj = ["open", "high", "low", "close", "pre_close", "vwap", "amt"]
        
        for varName in VarList_adj:
            
            print(varName)
            
            df = self.readWindCsv(varName, self.stockCode, self.tradeDay)
            adjFactor.index = df.index
            df = df*(adjFactor / np.array(adjFactor.max(axis = 0)))
            self.saveToCsv(df, varName)
            
            self.chenck_data_valid(varName)
            
        # not need adjfactors    
        VarList = ["dealnum", "volume","free_turn"]
        
        for varName in VarList:
            
            print(varName)
            
            df = self.readWindCsv(varName, self.stockCode, self.tradeDay)
            self.saveToCsv(df, varName)
            self.chenck_data_valid(varName)
        
        
        # calcualte returns
            
#        VarList = ['pct_chg']
#        for varName in VarList:
#            print(varName)
#            df = self.readWindCsv(varName)/100
#            self.saveToCsv(df, 'returns')   
        
        
        close_df  = self.readWindCsv("close", self.stockCode, self.tradeDay)
        
        returns_df = close_df/close_df.shift(1) - 1
        self.saveToCsv(returns_df, 'returns')
        self.chenck_data_valid('returns')
        
        
# =============================================================================
#         
# =============================================================================
    def generate_industry_matrix(self, 
                               varList= ['sector', 'industry','subindustry','miniindustry']):
        
        for varName in varList:
            
            print(varName)
            
            var = self.readWindCsv(varName, self.stockCode, self.tradeDay)
            
            var[var == np.nan] = -1
            
            self.saveToCsv(var, varName)
            
            self.chenck_data_valid(varName)
        
# =============================================================================
#         
# =============================================================================
    def chenck_data_valid(self, fileName):
        
        path = os.path.join(self.OutputDir, fileName + '.csv')
        df = pd.read_csv(path, index_col = 0)
        
        print(df.shape)
        
        #plot
        plt.figure(1, figsize=(10, 3))
        
        df[df == 0] = np.nan
        
        dataCoverage = df.count(axis = 1)
        
        dataCoverage.plot(grid = True, title = fileName)
        
        plt.show()
        
        return dataCoverage
    

        
    
    
if __name__ =='__main__':
    
    
    temp = DataProcess_Wind()
    
# =============================================================================
#   batch processing
# =============================================================================
    temp.generate_data_matrix()    
    temp.generate_industry_matrix(['sector', 'industry', 'subindustry'])    
    temp.generate_valid_matirx()
    
    
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
#    temp.chenck_data_valid('open')
#    temp.chenck_data_valid('high')
#    temp.chenck_data_valid('low')
    
#    ss= temp.chenck_data_valid('industry')
#    
#    
    
    
    
    
    
    
    
    
    
    
    
    
    
    


# -*- coding: utf-8 -*-
"""
Created on Thu Aug 24 13:44:33 2017

@author: chenxi
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Aug 10 14:21:44 2017

@author: chenxi
"""
import os
import pandas as pd
from WindPy import *
import datetime,time

class AlphaBT_WindApi(object):
    
    def __init__(self):
        
        # wind api start
        w.start()
        
        # change this path
        self.OutputDir = r'F:\Quant\AlphaBT\TempData\temp'
        
        
        self.startDateStr = '2011-01-01'
        self.endDateStr = '2017-08-10'
        
        self.get_StockList(saveOrNote=True)
        self.get_Trade_Day_data(saveOrNote=True)
        
# =============================================================================
# get tradeday list and save tradeday list
# =============================================================================
    def get_Trade_Day_data(self, saveOrNote = False):
        
        def dateTimeToStr(inDateTime):
            
            return datetime.datetime.strftime(inDateTime, '%Y-%m-%d')
        
        wData = w.tdays(beginTime= self.startDateStr,
                        endTime = self.endDateStr,
                        Days= 'Trading')
        
        if wData.ErrorCode == 0:
            
            tradeDayList = wData.Data[0]
            tradeDayList = pd.DataFrame(data = tradeDayList, columns = ['tradeDay'])
            tradeDayList['tradeDay'] = tradeDayList['tradeDay'].apply(dateTimeToStr)
            
            self.tradeDayList = tradeDayList
            
            if saveOrNote is True:
                
                path = os.path.join(self.OutputDir, 'tradeDayList.csv')
                tradeDayList.to_csv(path, header = True)
                
            else :  
                
                pass
                
        else:
            
            print("ErrorCode %d" %(wData.ErrorCode))
            
            self.tradeDayList = None

# =============================================================================
# get current stock list 
# =============================================================================    
    def get_StockList(self, saveOrNote = False):
        
        wData= w.wset("sectorconstituent","date="+ self.endDateStr +";sectorid=a001010100000000; field=wind_code")
        
        if wData.ErrorCode == 0:
            
            stockList = wData.Data[0]
            self.stockList_ls = stockList
            
            stockList = pd.DataFrame(data = stockList, columns = ['stockCode'])
            self.stockList_df = stockList
            
            
            if saveOrNote is True:
                
                path = os.path.join(self.OutputDir, 'stockCode.csv')
                stockList.to_csv(path, header = True)
                
            else :  
                
                pass
            
        
        else:
            
            print("ErrorCode %d" %(wData.ErrorCode))
            
            self.stockList = None
            
            
# =============================================================================
# get single wind var data    
# =============================================================================
    
    def getSingleData(self, varName, start_date, end_date, stockCodeList):
        
        temp = w.wsd(stockCodeList, varName, start_date, end_date, "") #filled methods = original 

        data = temp.Data

        data_df = pd.DataFrame(data,index=stockCodeList,columns=temp.Times).T
        
        data_df = self.trim_data_matrix(data_df)
        
        return data_df
    
# =============================================================================
#  get batch hist data
# =============================================================================
    def getHistData(self, varList):
        
        startYearNum = int(self.startDateStr[0:4])
        endYearNum = int(self.endDateStr[0:4])
        
        for var in varList:
            
            print(var)
            
            data_df = None
            
            for iYear in range(startYearNum, endYearNum+1):
                
                time.sleep(5)
                
                print(iYear)
                
                start_date = max(str(iYear) + '-01-01', self.startDateStr)
                end_date = min(str(iYear) + '-12-31', self.endDateStr)
                
                    
                df_temp = self.getWindSingleData(var, start_date, end_date, self.stockList_ls)
                
                if data_df is None:
                    
                    data_df = df_temp
                else:
                    
                    data_df = pd.concat([data_df,df_temp])
                        

            fileName = var + '.csv'
            filePath = os.path.join(self.OutputDir, fileName)
            
            data_df = self.trim_data_matrix(data_df)
            
            data_df.to_csv(filePath, header = True)
            
# =============================================================================
#  get all 4 industry files
# =============================================================================
    def getIndustryData(self, varList = ["sector", "industry", "subindustry", "miniindustry"]):
        
        startYearNum = int(self.startDateStr[0:4])
        endYearNum = int(self.endDateStr[0:4])
        
        for var in varList:
            
            print(var)
            
            data_df = None
            
            for iYear in range(startYearNum, endYearNum + 1):
                
                time.sleep(5)
                
                
                
                start_date = max(str(iYear) + '-01-01', self.startDateStr)
                end_date = min(str(iYear) + '-12-31', self.endDateStr)
                
                print(start_date + '...' + end_date)
                
                df_temp = self.getWindIindustry(var, start_date, end_date, self.stockList_ls)
                
                if data_df is None:
                    
                    data_df = df_temp
                
                else:
                    
                    data_df = pd.concat([data_df,df_temp])
                        

            data_df = self.trim_data_matrix(data_df)
            
            fileName = var + '.csv'
            filePath = os.path.join(self.OutputDir, fileName)
            
            data_df.to_csv(filePath, header = True)  
            
            
# =============================================================================
#     sub function for get industry function
# =============================================================================
    
    def getWindIindustry(self, varName, start_date, end_date, stockCodeList):
        
        if varName == 'sector':
            
            temp = w.wsd(stockCodeList, 'industry_gicscode', start_date, end_date, "industryType=1;Fill=Previous")
            
        elif varName == 'industry':
            
            temp = w.wsd(stockCodeList, 'industry_gicscode', start_date, end_date, "industryType=2;Fill=Previous")
            
        elif varName == 'subindustry':
            
            temp = w.wsd(stockCodeList, 'industry_gicscode', start_date, end_date, "industryType=3;Fill=Previous")
            
        elif varName == 'miniindustry':
            
            temp = w.wsd(stockCodeList, 'industry_gicscode', start_date, end_date, "industryType=4;Fill=Previous")
        
        data = temp.Data

        data_df = pd.DataFrame(data,index=stockCodeList,columns=temp.Times).T
        
        return data_df
# =============================================================================
# trim data
# =============================================================================
    def trim_data_matrix(self, df):
                
        def dateTimeToStr(inDateTime):
            
            return datetime.datetime.strftime(inDateTime, '%Y-%m-%d')
        
        df.index = pd.DataFrame(df.index)[0].apply(dateTimeToStr)
        
#        df = df.ix[self.tradeDayList['tradeDay']]
        
        return df
        
        
        
        
        
        
# =============================================================================
#  data processing for alphaBT
# =============================================================================
if __name__ =='__main__':

    temp = AlphaBT_WindApi()
    
#    temp.get_Trade_Day_data(saveOrNote=True)
    
#    temp.get_StockList(saveOrNote=True)
    
    temp.getIndustryData(['industry'])
    
#        varList = ['open', 'high', 'low', 'close']
#                   'pre_close','open','high','low','close','volume','amt', 'dealnum',
#        varList = ['chg','pct_chg','swing','vwap','adjfactor','close2',
#                   'turn','free_turn','lastradeday_s','last_trade_day',
#                   'rel_ipo_chg','rel_ipo_pct_chg','trade_status','susp_reason',
#                   'close3']
#        self.getWindSingleData('adjfactor', start_date, end_date, stockCodeList)
#        varList = ['adjfactor']
        # "trade_status,susp_reason,mf_amt,mf_vol,mf_amt_ratio,mf_vol_ratio,mf_amt_close,mf_amt_open,ev,mkt_cap_ard,pe_ttm,val_pe_deducted_ttm,pe_lyr,pb_lf,pb_mrq,ps_ttm"
    
#    varList = ["mf_amt"]    
    
#    temp.getHistData('TOP1500')

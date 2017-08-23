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
        w.start()
        # change this path
        self.OutputDir = r'F:\Quant\AlphaBT\TempData\WindData_bfq'
        
        
    def getCurrentTime(self):
        # 获取当前时间
        return time.strftime('%Y-%m-%d', time.localtime(time.time))
    
    
    def getStockList(self, saveOrNote = True):
        
        end_date = '2017-12-31'
        
        stockCodes= w.wset("sectorconstituent","date="+end_date+";sectorid=a001010100000000;field=wind_code")
        
        stockList = stockCodes.Data[0]
        
        stockList_df = pd.DataFrame(data = stockList, columns = ['stockCode'])
        
        self.stockList = stockList
        self.stockList_df = stockList_df
        
        if saveOrNote:
            
            path = os.path.join(self.OutputDir, 'stockCode' + '.csv')
            self.stockList_df.to_csv(path, header = True)
        else:
            
            pass
        
        return stockList, stockList_df
        
        
                                                                 
                                                                 
    def getWindHistData(self):
        
        def trimStr(data):
            
            data = str(data)
            
            return data[0:10]
        
        curr_date = self.getCurrentTime
        
        start_date = '2017-01-01'
        end_date = '2017-12-31'
        
        
        stockCodes=w.wset("sectorconstituent","date="+end_date+";sectorid=a001010100000000;field=wind_code")
        
        stockCodeList = stockCodes.Data[0]
        
#        varList = ['open', 'high', 'low', 'close']
#                   'pre_close','open','high','low','close','volume','amt', 'dealnum',
#        varList = ['chg','pct_chg','swing','vwap','adjfactor','close2',
#                   'turn','free_turn','lastradeday_s','last_trade_day',
#                   'rel_ipo_chg','rel_ipo_pct_chg','trade_status','susp_reason',
#                   'close3']
#        self.getWindSingleData('adjfactor', start_date, end_date, stockCodeList)
#        varList = ['adjfactor']
        # "trade_status,susp_reason,mf_amt,mf_vol,mf_amt_ratio,mf_vol_ratio,mf_amt_close,mf_amt_open,ev,mkt_cap_ard,pe_ttm,val_pe_deducted_ttm,pe_lyr,pb_lf,pb_mrq,ps_ttm"
        varList = ["mf_amt"]
        
        for var in varList:
            print(var)
            data_df = None
            
            for iYear in range(2011,2017+1):
                time.sleep(5)
                print(iYear)
                start_date = str(iYear) + '-01-01'
                end_date = str(iYear) + '-12-31'
                df_temp = self.getWindSingleData(var, start_date, end_date, stockCodeList)
                
                if data_df is None:
                    data_df = df_temp
                else:
                    data_df = pd.concat([data_df,df_temp])
                        

            fileName = var + '.csv'
            filePath = os.path.join(self.OutputDir, fileName)
            
            data_df.index = data_df.index.map(trimStr)
            
            data_df.to_csv(filePath, header = True)
            
            
            
            
            
    def getWindSingleData(self, varName, start_date, end_date, stockCodeList):
        
        temp = w.wsd(stockCodeList, varName, start_date, end_date, "") #filled methods = original 

        data = temp.Data

        data_df = pd.DataFrame(data,index=stockCodeList,columns=temp.Times).T
        
        return data_df
    
    
# =============================================================================
#     
# =============================================================================
    def getIndustryData(self):
        
        def trimStr(data):
            
            data = str(data)
            
            return data[0:10]
        
        end_date = '2017-12-31'
        
        
        stockCodes = w.wset("sectorconstituent","date="+end_date+";sectorid=a001010100000000;field=wind_code")
        
        stockCodeList = stockCodes.Data[0]
        
        varList = ["sector", "industry", "subindustry", "miniindustry"]
        
        for var in varList:
            print(var)
            data_df = None
            
            for iYear in range(2011,2017+1):
                time.sleep(5)
                print(iYear)
                start_date = str(iYear) + '-01-01'
                end_date = str(iYear) + '-12-31'
                df_temp = self.getWindIindustry(var, start_date, end_date, stockCodeList)
                
                if data_df is None:
                    data_df = df_temp
                else:
                    data_df = pd.concat([data_df,df_temp])
                        

            fileName = var + '.csv'
            filePath = os.path.join(self.OutputDir, fileName)
            
            data_df.index = data_df.index.map(trimStr)
            
            data_df.to_csv(filePath, header = True)    
    
    
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
#  data processing for alphaBT
# =============================================================================
    
    
if __name__ =='__main__':

    temp = AlphaBT_WindApi()
#    temp.getWindHistData()
    temp.getIndustryData()
#    stockList, stockList_df = temp.getStockList()
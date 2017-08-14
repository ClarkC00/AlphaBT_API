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
                                                                 
                                                                 
    def getWindHistData(self):
        
        curr_date = self.getCurrentTime
        
        start_date = '2017-01-01'
        end_date = '2017-12-31'
        
        
        stockCodes=w.wset("sectorconstituent","date="+end_date+";sectorid=a001010100000000;field=wind_code")
        
        stockCodeList = stockCodes.Data[0]
        
#        varList = ['open', 'high', 'low', 'close']
#'pre_close','open','high','low','close','volume','amt', 'dealnum',
#        varList = ['chg','pct_chg','swing','vwap','adjfactor','close2',
#                   'turn','free_turn','lastradeday_s','last_trade_day',
#                   'rel_ipo_chg','rel_ipo_pct_chg','trade_status','susp_reason',
#                   'close3']
#        self.getWindSingleData('adjfactor', start_date, end_date, stockCodeList)
        varList = ['adjfactor']
        
        for var in varList:
            print(var)
            data_df = None
            
            for iYear in range(2010,2017+1):
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
            data_df.to_csv(filePath, header = True)
            
    def getWindSingleData(self, varName, start_date, end_date, stockCodeList):
        
        temp = w.wsd(stockCodeList, varName, start_date, end_date, "") #filled methods = original 

        data = temp.Data

        data_df = pd.DataFrame(data,index=stockCodeList,columns=temp.Times).T
        
        return data_df 


# =============================================================================
#  data processing for alphaBT
# =============================================================================
    
    
if __name__ =='__main__':

    temp = AlphaBT_WindApi()
    temp.getWindHistData()
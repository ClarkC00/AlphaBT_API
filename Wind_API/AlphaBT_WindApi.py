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
        
        self.OutputDir = r'F:\Quant\AlphaBT\TempData'
        
        
    def getCurrentTime(self):
        # 获取当前时间
        return time.strftime('%Y-%m-%d', time.localtime(time.time))
                                                                 
                                                                 
    def getWindHistData(self):
        
        curr_date = self.getCurrentTime
        
        start_date = '2011-01-01'
        end_date = '2017-08-09'
        
        
        stockCodes=w.wset("sectorconstituent","date="+end_date+";sectorid=a001010100000000;field=wind_code")
        
        stockCodeList = stockCodes.Data[0]
        
#        varList = ['open', 'high', 'low', 'close']
        varList = ['pre_close','open','high','low','close','volume','amt',
                   'dealnum','chg,pct_chg','swing','vwap','adjfactor','close2',
                   'turn','free_turn','lastradeday_s','last_trade_day',
                   'rel_ipo_chg','rel_ipo_pct_chg','trade_status','susp_reason',
                   'close3']
        
        for var in varList:
            self.getWindSingleData(var, start_date, end_date, stockCodeList)


    def getWindSingleData(self, varName, start_date, end_date, stockCodeList):
        
        temp = w.wsd(stockCodeList, varName, start_date, end_date, "")

        data = temp.Data

        data_df = pd.DataFrame(data,index=stockCodeList,columns=temp.Times).T
        
        fileName = varName + '.csv'
        filePath = os.path.join(self.OutputDir, fileName)
        data_df.to_csv(filePath, header = True)
    
    
    
if __name__ =='__main__':

    temp = AlphaBT_WindApi()
    temp.getWindHistData()
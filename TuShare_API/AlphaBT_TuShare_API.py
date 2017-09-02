# -*- coding: utf-8 -*-
"""
Created on Fri Sep  1 15:54:34 2017

@author: chenxi
"""
import tushare as ts
import pandas as pd

l = pd.DataFrame(['002337','000001'])
a = ts.get_hist_data(l) #一次性获取全部日k线数据
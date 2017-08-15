# -*- coding: utf-8 -*-
"""
Created on Tue Aug 15 10:44:39 2017

@author: chenxi
"""

from sas7bdat import SAS7BDAT


infile = r'F:\Quant\AlphaBT\DataBase3\Future\Future_SAS_files\sh50_idx_mwt.sas7bdat'
with SAS7BDAT(infile) as f:
    df = f.to_data_frame()
        

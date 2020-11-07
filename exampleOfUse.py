# -*- coding: utf-8 -*-
"""
Created on Mon Nov  2 21:14:43 2020

@author: guiii
"""

from envcanlib import downloadData, to_sql, from_sql
import pandas as pd

conn_string = "DRIVER={ODBC Driver 13 for SQL Server};\
                                 SERVER=tcp:open-data.database.windows.net;\
                                     DATABASE=environment_canada_db;\
                                         UID={ubilab};\
                                             PWD={Mandioca78}"

downloadData(['155'], start=(2018,2), end=(2018,6))
df = pd.read_csv('155.csv')

to_sql(df, conn_string, 'new_function_test', if_exists='append')

new_df = from_sql(conn_string,
'''
SELECT * FROM [dbo].[stations_inventory] WHERE Province = 'QUEBEC'
''')
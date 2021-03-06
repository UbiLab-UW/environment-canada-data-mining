# -*- coding: utf-8 -*-
"""
Created on Mon Nov  2 21:14:43 2020

@author: guiii
"""

from envcanlib import getData, to_sql, from_sql
import pandas as pd

conn_string = "DRIVER={ODBC Driver 13 for SQL Server};\
                                 SERVER=xxxxxxxx;\
                                     DATABASE=xxxxx;\
                                         UID={xxxxx};\
                                             PWD={xxxxx}"

getData(['150','140'], start=(2018,2), end=(2018,6), saveFile=False, 
        db_conn_string=conn_string, db_table_name='hourly')

df = pd.read_csv('155.csv')

to_sql(df, conn_string, 'new_function_test', if_exists='append')

new_df = from_sql(conn_string, 'stations_inventory')
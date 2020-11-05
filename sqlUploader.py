# -*- coding: utf-8 -*-
"""
Created on Mon Nov  2 21:14:43 2020

@author: guiii
"""

import urllib
import envcanlib as ecl
from sqlalchemy import event, create_engine
import pandas as pd

params = urllib.parse.quote_plus("DRIVER={ODBC Driver 13 for SQL Server};\
                                 SERVER=tcp:open-data.database.windows.net;\
                                     DATABASE=environment_canada_db;\
                                         UID={ubilab};\
                                             PWD={Mandioca78}")

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, pool_pre_ping=True)

#It makes the upload faster
@event.listens_for(engine, 'before_cursor_execute')
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    if executemany:
        cursor.fast_executemany = True
        cursor.commit()

conn = engine.connect()

ecl.downloadData(['150'], start=(2018,2), end=(2018,6))
df = pd.read_csv('150.csv')

df.to_sql(name='teste2', con=conn, if_exists='append')
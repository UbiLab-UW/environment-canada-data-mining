#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 30 23:35:15 2019

@author: guilherme
"""

def getData(IDs, start, end, method = 'hourly', path = '', dataFormat = 'default', 
            continuous = True, metaData = None,  fileName = None, saveFile = True,
            db_conn_string = None, db_table_name = None, if_exists_table = 'append'):
    '''
    Description: It downloads weather information from the Environment Canada website. 
    It is possible to download daily or hourly information in a slice of time passed as an argument.
    
    Input: 
        IDs:  list
            list of the target stations IDs.
    
        start: tuple, list
            A tuple with start year and start month.
           
        end: tuple,list
            A tuple with end year and end month.
           
        method: string
            'hourly' for hourly information (default) or 'daily' for daily information.
           
        path: string
            Path in the machine to save the data downloaded. Default is the path where the code is running.
            To use this option, make sure saveFile argument is True.
           
        dataFormat: string
            'default' (each station has its own file) or 'oneFile' (just one file for all stations).
           
        continuous: bool
            If True the time passed will be considered as continuous, otherwise only months betwwen 
            start month and end month will be downloaded. Default value is True.
        
        metaData: pandas.DataFrame, optional
            Dataframe containing information about the stations.
        
        fileName: string, optional
            Dictionary to name each file.
           
        saveFile: bool, optional
            If True then the dataframes will be saved in the machine.
            
        db_conn_string: string, optional
            Connection string to connect to a server and then upload the data to it.
            
        db_table_name: string, optional
            Name of the table to upload the data in case conn_string has been passed as well.
            
        if_exists_table: string
            Decides what to do if already exists the table given. "append" will append the data to the table.
            "replace" will create a new table. "fail" will raise an error. This have effect in case 
            db_table_name and conn_string has been passed as well.
            
    Output: None
        This function returns nothing.
    '''
    
    import pandas as pd
    import urllib.request as url
    
    if start[0] > end[0]:
        raise ValueError('Start year is greater than end year')
    if start[0] == end[0] and start[1] > end[1]:
        raise ValueError('Start month is greater than end month')
    
    
    if method == 'hourly':
        method  = "&timeframe=1&submit=Download+Data"
        downloadMethod = 'HLY'
    elif method == 'daily':
        method  = "&timeframe=2&submit=Download+Data"
        downloadMethod = 'DLY'
    else:
        raise('method = ' + method + 'is not valid. Avalible methods are "h" or "d".')
    
    if dataFormat == 'default':
        for ID in IDs:
            data = pd.DataFrame([])
            for intYr in range(start[0], end[0]+1):
                if continuous:
                    if intYr == start[0]:
                        if start[0] == end[0]:
                            monthRange = range(start[1], end[1]+1)
                        else:
                            monthRange = range(start[1],13)
                    elif intYr == end[0]:
                        monthRange = range(1, end[1]+1)
                    else:
                        monthRange = range(1, 13)
                else:
                    monthRange = range(start[1],end[1]+1)
                
                if method == "&timeframe=1&submit=Download+Data":
                    for intMnt in monthRange:
                        #build the query
                        strQry = 'http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=' + str(ID) + "&Year=" + str(intYr) +'&Month=' + str(intMnt) + method 
                        #print strQry
                        #print ('Querying station ' + str(ID) + ' for year ' + str(intYr) + ' and month ' + str(intMnt))
                        try:
                            response = url.urlopen(strQry)
                            rawData = response.readlines()
                            response.close()
                            rawData = [row.decode('utf8').replace('"','').replace('\n','') for row in rawData]
                           
                            columns = rawData[0].split(',')
                            d = [line.split(',') for line in rawData[1:]]
                            
                            for i in range(len(d)):
                                if len(d[i]) > len(columns):
                                    d[i][len(columns)-1] = "".join(d[i][len(columns)-1:])
                                    d[i] = d[i][:len(columns)]
                                    
                                if len(d[i]) < len(columns):
                                    while len(d[i]) < len(columns):
                                        d[i].append('')
                                    
                            newData = pd.DataFrame(d, columns=columns)
                            data = data.append(newData, ignore_index=True, sort=False)
                        except Exception:
                            pass
                            #print ('Failure getting data for '  + str(ID) + ' for year ' + str(intYr) + '. ',e)
                else:
                    #build the query
                    strQry = 'http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=' + str(ID) + "&Year=" + str(intYr) + method 
                    #print strQry
                    #print ('Querying station ' + str(ID) + ' for year ' + str(intYr))
                    try:
                        response = url.urlopen(strQry)
                        rawData = response.readlines()
                        response.close()
                        rawData = [row.decode('utf8').replace('"','').replace('\n','') for row in rawData]
                       
                        columns = rawData[0].split(',')
                        d = [line.split(',') for line in rawData[1:]]
                        
                        for i in range(len(d)):
                            if len(d[i]) > len(columns):
                                d[i][len(columns)-1] = "".join(d[i][len(columns)-1:])
                                d[i] = d[i][:len(columns)]
                                
                            if len(d[i]) < len(columns):
                                while len(d[i]) < len(columns):
                                    d[i].append('')
                                
                        newData = pd.DataFrame(d, columns=columns)
                        data = data.append(newData, ignore_index=True, sort=False)
                        data['Month'] = data['Month'].astype(int)
                        data['Year'] = data['Year'].astype(int)
                        if start[0] != end[0]:
                            data = data[((data['Month'] >= start[1]) & (data['Year'] == start[0]))
                                        | ((data['Year'] != start[0]) & (data['Year'] != end[0]))
                                        | ((data['Month'] <= end[1]) & (data['Year'] == end[0]))]
                        else:
                            data = data[(data['Month'] >= start[1]) & (data['Month'] <= end[1])]
                            
                        if not continuous:
                            data = data[(data['Month'] <= end[1]) & (data['Month'] >= start[1])]
    
                    except Exception:
                        pass
                        #print ('Failure getting data for '  + str(ID) + ' for year ' + str(intYr))
            
            data = data.dropna(axis = 0, how = 'all')
            data['Station ID'] = ID
            
            if saveFile:
                if type(fileName) == dict:
                    data.to_csv(path+fileName[str(ID)]+".csv", index=False, line_terminator="")
                else:
                    data.to_csv(path+str(ID)+".csv", index=False, line_terminator="")
                    
            if type(db_conn_string) == str and type(db_table_name) == str:
                to_sql(data, db_conn_string, db_table_name, if_exists_table)
            
    else:
        data = pd.DataFrame([])
        for ID in IDs:
            for intYr in range(start[0], end[0]+1):
                if continuous:
                    if intYr == start[0]:
                        if start[0] == end[0]:
                            monthRange = range(start[1], end[1]+1)
                        else:
                            monthRange = range(start[1],13)
                    elif intYr == end[0]:
                        monthRange = range(1, end[1]+1)
                    else:
                        monthRange = range(1, 13)
                else:
                    monthRange = range(start[1],end[1]+1)    
                
                if method == "&timeframe=1&submit=Download+Data":
                    for intMnt in monthRange:
                        #build the query
                        strQry = 'http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=' + str(ID) + "&Year=" + str(intYr) +'&Month=' + str(intMnt) + method 
                        #print strQry
                        #print ('Querying station ' + str(ID) + ' for year ' + str(intYr) + ' and month ' + str(intMnt))
                        try:
                            response = url.urlopen(strQry)
                            rawData = response.readlines()
                            response.close()
                            rawData = [row.decode('utf8').replace('"','').replace('\n','') for row in rawData]
                           
                            columns = rawData[0].split(',')
                            d = [line.split(',') for line in rawData[1:]]
                            
                            for i in range(len(d)):
                                if len(d[i]) > len(columns):
                                    d[i][len(columns)-1] = "".join(d[i][len(columns)-1:])
                                    d[i] = d[i][:len(columns)]
                                    
                                if len(d[i]) < len(columns):
                                    while len(d[i]) < len(columns):
                                        d[i].append('')
                            
                            newData = pd.DataFrame(d, columns=columns)
                            newData.insert(0, column='Station ID', value = [ID for i in range(newData.shape[0])])
                            
                            if type(metaData) == pd.DataFrame:
                                province = metaData.loc[metaData['Station ID'] == str(ID)]['Province'].values[0]
                                newData.insert(1, column='Province', value = [province for i in range(newData.shape[0])])
                            
                            data = data.append(newData, ignore_index=True, sort=False)
                        except Exception:
                            pass
                            #print ('Failure getting data for '  + str(ID) + ' for year ' + str(intYr) + '. ',e)
                else:
                    #build the query
                    strQry = 'http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=' + str(ID) + "&Year=" + str(intYr) + method 
                    #print strQry
                    #print ('Querying station ' + str(ID) + ' for year ' + str(intYr))
                    try:
                        response = url.urlopen(strQry)
                        rawData = response.readlines()
                        response.close()
                        rawData = [row.decode('utf8').replace('"','').replace('\n','') for row in rawData]
                       
                        columns = rawData[0].split(',')
                        d = [line.split(',') for line in rawData[1:]]
                        
                        for i in range(len(d)):
                            if len(d[i]) > len(columns):
                                d[i][len(columns)-1] = "".join(d[i][len(columns)-1:])
                                d[i] = d[i][:len(columns)]
                                
                            if len(d[i]) < len(columns):
                                while len(d[i]) < len(columns):
                                    d[i].append('')
                                
                        newData = pd.DataFrame(d, columns=columns)
                        newData.insert(0, column='Station ID', value = [ID for i in range(newData.shape[0])])
                        
                        if type(metaData) == pd.DataFrame:
                            province = metaData.loc[metaData['Station ID'] == str(ID)]['Province'].values[0]
                            newData.insert(1, column='Province', value = [province for i in range(newData.shape[0])])
                        
                        data = data.append(newData, ignore_index=True, sort=False)
                        data['Month'] = data['Month'].astype(int)
                        data['Year'] = data['Year'].astype(int)
                        if (start[0] != end[0]) and continuous:
                            data = data[((data['Month'] >= start[1]) & (data['Year'] == start[0]))
                                        | ((data['Year'] != start[0]) & (data['Year'] != end[0]))
                                        | ((data['Month'] <= end[1]) & (data['Year'] == end[0]))]
                        else:
                            data = data[(data['Month'] >= start[1]) & (data['Month'] <= end[1])]
    
                    except Exception:
                        pass
                        #print ('Failure getting data for '  + str(ID) + ' for year ' + str(intYr))
            
        data = data.dropna(axis = 0, how = 'all')
        
        if saveFile:
            data.to_csv(path+downloadMethod+'Information.csv', index=False, line_terminator="")
            
        if type(db_conn_string) == str and type(db_table_name) == str:
            to_sql(data, db_conn_string, db_table_name, if_exists_table)

def to_sql(dataframe, conn_string, table_name, if_exists = 'append'):
    '''
    Upload dataframe to sql server using pyodbc.
    
    dataframe: pandas.DataFrame
        Dataframe containing the data.
        
    conn_string: string
        Connection string to connect with the database.
    
    table_name: string
        Name of the table to add the data.
    
    if_exists: string
        Decides what to do if already exists the table given. "append" will append the data to the table.
        "replace" will create a new table. "fail" will raise an error.
    
    Output: None
        It returns nothing.
    '''
    
    import urllib
    import sqlalchemy as sa
    from sqlalchemy import event, create_engine
    
    
    params = urllib.parse.quote_plus(conn_string)
    
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, pool_pre_ping=True)
    
    #It makes the upload faster
    @event.listens_for(engine, 'before_cursor_execute')
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        if executemany:
            cursor.fast_executemany = True
            cursor.commit()
    
    i = 0
    while i < 10:
        try:
            conn = engine.connect()
            break
        except sa.exc.InterfaceError:
            i+=1
    
    dataframe.to_sql(name=table_name, con=conn, if_exists=if_exists)
    conn.close()
    
def from_sql(conn_string, table_name):
    '''
    Get Data from sql server from a specific table
    
    conn_string: string
        Connection string to connect with the database.
        
    table_name: string
        Name of the desired table to get the data.
        
    Output: pandas.DataFrame
        It retunrs a dataframe containing the target data.
    '''
    
    import urllib
    import sqlalchemy as sa
    from sqlalchemy import create_engine
    import pandas as pd
    
    sql_command = '''SELECT * FROM [dbo].[%s] WHERE Province = 'QUEBEC' ''' %table_name
    params = urllib.parse.quote_plus(conn_string)
    
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, pool_pre_ping=True)
    
    i = 0
    while i < 10:
        try:
            conn = engine.connect()
            break
        except sa.exc.InterfaceError:
            i+=1
    
    SQL_Query = pd.read_sql_query(sql_command, conn)
    conn.close()
    
    df = pd.DataFrame(SQL_Query)
    df.columns = SQL_Query.keys()
    
    return df
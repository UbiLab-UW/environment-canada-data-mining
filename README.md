# Environment Canada Data Mining
Python3 package to download information from Environment Canada: Weather Information

### Authors

**Guilherme de Brito Abreu** - Student at University of Campinas

## Description
This is a python3 package of functions developed in order to assist in the processing of canadian weather information.

### Functions Available

**envcanlib.downloadData**(IDs, start, end, method='hourly', path='', dataFormat='default', continuous=True,
metaData=None)

    Pull daily and hourly information from the Government of Canada official website of a specified period of time and save the data in the disk.

    Parameters:

        IDs : list of strings, array_like as str type
            List of staions  IDs.

        start : tuple as int type
            A tuple containing year and month to start.
        
        end : tuple as int type  
            A tuple containing year and month to end.

        method : string, optional  
            'hourly' for hourly information and 'daily' for daily information.

        path : string, optional 
            Path to save the data in the disk. The default directory is the root directory.

        dataFormat: string, optional
            'default' means that it will be saved one file for each ID in IDs. 'oneFile' means that
            one file containing all pulled information will be saved.

        continuous: bool, optional
            If True all information between start and end will be pulled, otherwise it will be pulled just the information between start month and end month from start year until end year. 
            For instance, if start = (2014,5) and end = (2015,7) and continuous = False, then it will be pulled just the months 5,6 and 7 of 2014 and 2015. If continuous = True, all the data between March 2014 and July 2015 will be pulled. 
            This can be useful if it is desired to have slices of a period of time.

        metaData: pandas.DataFrame, optional
            In case dataFormat is passed as 'oneFile' this dataframe will be used to get more information about the stations. This parameter is always optional.
            
        fileName: dict, optional 
            Dictionary to name each file. Format: {'ID1':filename1, 'ID2':filename2}
            
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
        It returns nothing.

**to_sql**(dataframe, conn_string, table_name, if_exists = 'append'):

    Upload dataframe to sql server using pyodbc.
    
    Parameters:
    
        dataframe: pandas.DataFrame
            Dataframe containing the data.
            
        conn_string: string
            Connection string to connect to the database.
        
        table_name: string
            Name of the table to add the data.
        
        if_exists: string
            Decides what to do if already exists the table given. "append" will append the data to the table.
            "replace" will create a new table. "fail" will raise an error.
        
    Output: None
        It returns nothing.
        
**from_sql**(conn_string, table_name, table_rule=''):

    Get Data from sql server from a specific table
    
    Parameters:
    
        conn_string: string
            Connection string to connect to the database.
            
        table_name: string
            Name of the desired table to get the data.
        
        table_rule: string
            Rule to filter data. For instance, if you want all data where
            collumn Province has value equal "QUEBEC" then do 
            table_rule = 'WHERE Province = "QUEBEC"'. Default is a blank string.
        
    Output: pandas.DataFrame
        It retunrs a dataframe containing the target data.

## Dependencies

- Numpy
- Urllib3
- Pandas
- PyODBC
- MySQL
- SqlAlchemy
- ODBC driver

## Installation

In order to install it on Ubuntu systems just execute the following command in the root directory as superuser:
    **python3 install.py**

## Exemplo of use

You can find examples of how to use the functions in exampleOfUse.py

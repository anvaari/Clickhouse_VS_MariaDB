#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 25 19:10:40 2023

@author: anvaari
"""

from etc.exceptions import InsertError,ExecutionError


from clickhouse_driver import Client
import pandas as pd
import numpy as np

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def get_query_as_dataframe(query:str, clickhous_cred:dict) -> pd.DataFrame:
    '''
    Execute select query and return result as pandas dataframe

    Parameters
    ----------
    query : str
        Query to clickhouse server.
    clickhous_cred : dict
        dictioanry which contains host ip,port,username and password for connecting to clickhouse.

    Returns
    -------
    result : pd.DataFrame
        result as dataframe.

    '''
    
    with Client(**clickhous_cred,settings={'use_numpy': True}) as client:
        result = client.query_dataframe(query)
        
    return result

def insert_into_clickhouse_table(df:pd.DataFrame, table_name:str, clickhous_cred:dict) -> pd.DataFrame :
    '''
    Insert given dataframe into clickhouse table

    Parameters
    ----------
    df : pd.DataFrame
        dataframe which contains data, name of columns must match with database names.
    table_name : str
        name of table data must insert into it.
    clickhous_cred : dict
        dictioanry which contains host ip,port,username and password for connecting to clickhouse.

    Returns
    -------
    None.

    '''
    
    df = df.replace({np.nan:None})
    data = tuple(map(tuple,df.values))
    
    columns = f"{tuple([x for x in df.columns])}".replace("'","`")
    del df
    
    insert_query = f"INSERT INTO {table_name} {columns} values"
    
    with Client(**clickhous_cred) as client:
        try:
            client.execute(insert_query,data,types_check=True)
        except Exception as e:
            logger.error(f"Error while insert data into {table_name} in clickhouse\nError:{e}")
            raise InsertError
    logger.info(f"{len(data)} inserted into {table_name} in clickhouse")
    

def execute_query(query:str, clickhouse_cred:dict) -> None:
    """
    execute query on clickhouse server

    Parameters
    ----------
    query : str
        Query you want to execute.
    clickhouse_cred : dict
        dictioanry which contains host ip,port,username and password for connecting to clickhouse..

    Returns
    -------
    None

    """
    with Client(**clickhouse_cred) as client:
        try:
            client.execute(query)
        except Exception as e:
            logger.error(f"Can't execute query.\nError:{e}",exc_info=True)
            raise ExecutionError
logger.info("Query executed sucessfuly")
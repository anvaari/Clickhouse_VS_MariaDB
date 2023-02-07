#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 25 20:21:13 2023

@author: anvaari
"""

from etc.exceptions import InsertError,ExecutionError
import mysql.connector as mariadb
import pandas as pd
import numpy as np


import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def get_query_as_dataframe(query:str,columns_name:list, mariadb_cred:dict) -> pd.DataFrame:
    '''
    Execute select query and return result as pandas dataframe

    Parameters
    ----------
    query : str
        Query to mariadb server.
    columns_name : str
        name of columns which exist in query.
    mariadb_cred : dict
        dictioanry which contains host ip,port,username and password for connecting to mariadb.

    Returns
    -------
    result : pd.DataFrame
        result as dataframe.

    '''
    
    with mariadb.connect(**mariadb_cred) as connection:
        with connection.cursor() as cursor:
             cursor.execute(query)
             data = list(cursor)
    
    result = pd.DataFrame(data=data,columns=columns_name)   
    return result

def insert_into_mariadb_table(df:pd.DataFrame, table_name:str, mariadb_cred:dict) -> pd.DataFrame :
    '''
    Insert given dataframe into mariadb table

    Parameters
    ----------
    df : pd.DataFrame
        dataframe which contains data, name of columns must match with database names.
    table_name : str
        name of table data must insert into it.
    mariadb_cred : dict
        dictioanry which contains host ip,port,username and password for connecting to mariadb.

    Returns
    -------
    None.

    '''
    df = df.replace({np.nan:None})
    data = tuple(map(tuple,df.values))
    
    columns = f"{tuple([x for x in df.columns])}".replace("'","`")
    del df
    
    insert_query = f"INSERT INTO {table_name} {columns} values" + f"{tuple(['%s' for x in range(len(data[0]))])}".replace("'","")
    
    with mariadb.connect(**mariadb_cred) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
            try:
                cursor.executemany(insert_query,data)
                connection.commit()
            except mariadb.Error as e:
                connection.rollback()
                logger.error(f"Error while insert data into {table_name} in mariadb\nError:{e}")
                raise InsertError
    logger.info(f"{len(data)} inserted into {table_name} in mariadb")
    
def execute_query(query:str, mariadb_cred:dict):
    with mariadb.connect(**mariadb_cred) as connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute(query)
                connection.commit()
            except mariadb.Error as e:
                connection.rollback()
                logger.error(f"Can't execute query.\nError:{e}",exc_info=True)
                raise ExecutionError
    logger.info("Query executed sucessfuly")
    
def truncate_table(table_name:str,mariadb_cred:dict):
    with mariadb.connect(**mariadb_cred) as connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
                cursor.execute(f"truncate {table_name}")
                connection.commit()
            except mariadb.Error as e:
                connection.rollback()
                logger.error(f"Can't truncate {table_name}.\nError:{e}",exc_info=True)
                raise ExecutionError
    logger.info(f"{table_name} truncated sucessfuy")
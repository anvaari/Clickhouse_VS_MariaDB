#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 25 19:10:09 2023

@author: anvaari
"""

from utils.queries import clickhouse_tables , mariadb_tables
from utils.clickhouse_utils import execute_query as click_exec , insert_into_clickhouse_table
from utils.mariadb_utils import execute_query as maria_exec , insert_into_mariadb_table
from etc.constants import clickhouse_cred , project_path , base_log_format , normal_datetime_format , mariadb_cred , posts_columns,profiles_columns,locations_columns,post_types , CLICKHOUSE,MARIADB
from etc.exceptions import DataNotFoundError

import os
import logging
from datetime import datetime
import pandas as pd
import argparse

if not os.path.isdir(os.path.join(project_path,'logs')):
    os.mkdir(os.path.join(project_path,'logs'))
    
log_file_name = f"Clickhouse_VS_MariaDB.{datetime.now().strftime(normal_datetime_format)}.log"
log_file_path = os.path.join(project_path,'logs',log_file_name)
logging.basicConfig(filename=log_file_path,
                    level=logging.INFO,
                    format=base_log_format)

def create_tables(database):
    if database == CLICKHOUSE:
        for table in clickhouse_tables:
            click_exec(clickhouse_tables[table], clickhouse_cred)
            logging.info(f"{table} created sucsessfuly in clickhouse")
    elif database == MARIADB:
        for table in mariadb_tables:
            maria_exec(mariadb_tables[table], mariadb_cred)
            logging.info(f"{table} created sucsessfuly in mariadb")
    else:
        raise ValueError(f"database must be on of '{CLICKHOUSE}' or '{MARIADB}' but {database} given.")

def truncate_tables(database):
    tables_name =  ['profiles','locations','post_types','posts']
    for table in tables_name:
        if database == CLICKHOUSE:
            click_exec(f"truncate table {table}", clickhouse_cred)
            logging.info(f"{table} truncated sucsessfuly in clickhouse")
        elif database == MARIADB:
            maria_exec(f"truncate {table_name}", mariadb_cred)
            logging.info(f"{table} truncated sucsessfuly in mariadb")
        else:
            raise ValueError(f"database must be on of '{CLICKHOUSE}' or '{MARIADB}' but {database} given.")


def load_data_in_batch(chunk_size):
    data_folder_path  = os.path.join(project_path,'data')
    if not os.path.isdir(data_folder_path):
        raise DataNotFoundError(f"All csv files should be in data folder in project path : {project_path}")
    
    profiles_data_gen = pd.read_csv(os.path.join(data_folder_path, 'instagram_profiles.csv'),sep='	',usecols=profiles_columns.keys(),chunksize=chunk_size)
    for data in profiles_data_gen:
        data = data.rename(columns=profiles_columns)
        data.followings = data.followings.astype('Int64')
        data.followers = data.followers.astype('Int64')
        data.number_of_posts = data.number_of_posts.astype('Int64')

        yield ('profiles',data)

    location_data_gen = pd.read_csv(os.path.join(data_folder_path,'instagram_locations.csv'),sep='	',usecols=locations_columns.keys(),chunksize=chunk_size)
    for data in location_data_gen:
        data = data.rename(columns=locations_columns)
        yield ('locations',data)
        
    post_data_gen = pd.read_csv(os.path.join(data_folder_path, 'instagram_posts.csv'),sep='	',usecols=posts_columns.keys(),chunksize=chunk_size)
    for data in post_data_gen:
        data = data.rename(columns=posts_columns)
        data.comments = data.comments.astype('Int64')
        data.like = data.like.astype('Int64')
        data.location_id = data.location_id.astype('Int64')
        data.profile_id = data.profile_id.astype('Int64')
        data.date = pd.to_datetime(data.date)
        data = data.dropna(subset=['location_id','profile_id'])
        yield ('posts',data)
        
def insert_data_into_db(data:pd.DataFrame,db:str,table_name):
    insert_funcs = {CLICKHOUSE:insert_into_clickhouse_table,MARIADB:insert_into_mariadb_table}
    credentials = {CLICKHOUSE:clickhouse_cred,MARIADB:mariadb_cred}
    
    data['insert_date'] = datetime.now()
    insert_funcs[db](data,table_name,credentials[db])


def insert_post_type_into_db(db:str):
    insert_funcs = {CLICKHOUSE:insert_into_clickhouse_table,MARIADB:insert_into_mariadb_table}
    credentials = {CLICKHOUSE:clickhouse_cred,MARIADB:mariadb_cred}

    post_type_df = pd.DataFrame(columns=['id','name','insert_date'])
    post_type_df['id'] = post_types.keys()
    post_type_df['name'] = post_types.values()
    post_type_df['insert_date'] = datetime.now()
    
    insert_funcs[db](post_type_df,'post_types',credentials[db])
        
    logging.info("All data loaded sucessfuly")

if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(description="""Compare Clickhouse and MariaDB for OLAP queries.
                                     """)
    parser.add_argument(
    "-db",
    dest='database',
    help="Database which you want to run test for it",
    )
    parser.add_argument(
    "--chunk-size",
    dest='chunk_size',
    default=10000,
    help="Size of chunks for reading and inserting data.",
    )
    parser.add_argument(
    "--truncate",
    dest='truncate',
    action='store_true',
    help="Whether truncate all table before starting or not",
    )
    # Assign argumnets to variables 
    args = parser.parse_args()
    db=args.database
    chunk_size=args.chunk_size
    truncate = args.truncate

    create_tables(db)
    
    if truncate:
        truncate_tables(db)
        
    insert_post_type_into_db(db)
    for data_raw in load_data_in_batch(chunk_size):
        table_name,data = data_raw
        insert_data_into_db(data, db, table_name)
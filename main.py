#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 25 19:10:09 2023

@author: anvaari
"""

from utils.queries import clickhouse_tables , mariadb_tables
from utils.clickhouse_utils import execute_query as click_exec , insert_into_clickhouse_table
from utils.mariadb_utils import execute_query as maria_exec , insert_into_mariadb_table
from etc.constants import clickhouse_cred , project_path , base_log_format , normal_datetime_format , mariadb_cred , posts_columns,profiles_columns,locations_columns

import os
import logging
from datetime import datetime
import pandas as pd

if not os.path.isdir(os.path.join(project_path,'logs')):
    os.mkdir(os.path.join(project_path,'logs'))
    
log_file_name = f"Clickhouse_VS_MariaDB.{datetime.now().strftime(normal_datetime_format)}.log"
log_file_path = os.path.join(project_path,'logs',log_file_name)
logging.basicConfig(filename=log_file_path,
                    level=logging.INFO,
                    format=base_log_format)



        
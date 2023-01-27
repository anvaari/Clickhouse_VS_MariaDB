#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 25 19:11:02 2023

@author: anvaari
"""

import os
import pathlib

#Credentials
mariadb_cred = {'host':'127.0.0.1','port':3309, 'user':'root','password':'123456','database':'instagram'}
clickhouse_cred = {'host':'127.0.0.1','port':19000, 'user':'default','password':'','database':'instagram'}


# Log
# Log Format
base_log_format = "%(asctime)s - %(levelname)s in file %(filename)s function %(funcName)s line %(lineno)s : %(message)s"


# Specify path where this project exist
script_path=os.path.dirname(os.path.abspath(__file__))
project_path = str(pathlib.Path(script_path).parent.absolute())

# Time format
normal_datetime_format = "%Y-%m-%d %H:%M:%S"
date_format = "%Y-%m-%d"

# Data
posts_columns = {'sid':'id','sid_profile':'profile_id','location_id':'location_id','post_type':'post_type','cts':'date','numbr_likes':'like','number_comments':'comments'}    
profiles_columns = {'sid':'id','profile_name':'username','firstname_lastname':'name','followers':'followers','following':'followings','n_posts':'number_of_posts'}
locations_columns = {'id':'id','name':'name','city':'city','cd':'country_code'}
post_types = {1:'Photo',2:'Video',3:'Multi'}


# Constants
MARIADB = 'mariadb'
CLICKHOUSE = 'clickhouse'
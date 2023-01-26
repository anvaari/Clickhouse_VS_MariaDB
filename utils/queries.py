#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 25 20:37:40 2023

@author: anvaari
"""

post_table_create = """
CREATE TABLE IF NOT EXISTS instagram.posts
(

    `id` Int64,

    `profile_id` Int64,

    `location_id` Int64,

    `date` DateTime,

    `post_type` Uint8,

    `like` Uint32,

    `comments` UInt32,

    `insert_date` DateTime,

)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 8192;  
"""

profile_table_create = """
CREATE TABLE IF NOT EXISTS instagram.profiles
(

    `id` Int64,

    `username` String,

    `name` String,

    `followings` Uint32,

    `followers` Uint32,

    `number_of_posts` Uint32,

    `insert_date` DateTime,

)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 8192;  
"""
location_table_create = """
CREATE TABLE IF NOT EXISTS instagram.locations
(

    `id` Int64,

    `name` String,

    `city` String,

    `country_code` FixedString(2),

    `insert_date` DateTime,

)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 8192;  
"""
post_type_table_create = """
CREATE TABLE IF NOT EXISTS instagram.post_types
(

    `id` Uint8,

    `name` String,

    `insert_date` DateTime

)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 8192;  
"""

clickhouse_tables = {'posts':post_table_create,'profiles':profile_table_create,'locations':location_table_create,'post_types':post_type_table_create}


post_table_create = """
CREATE TABLE IF NOT EXISTS instagram.posts (
  `id` BIGINT NOT NULL  PRIMARY KEY,
  `profile_id` BIGINT NOT NULL REFERENCES profiles(id),
  `location_id` BIGINT NOT NULL REFERENCES locations(id),
  `date` DATETIME NOT NULL,
  `post_type` TINYINT NOT NULL REFERENCES post_types(id),
  `like` INT UNSIGNED NOT NULL,
  `comments` INT UNSIGNED NOT NULL,
  `insert_date` DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

"""

profile_table_create = """

CREATE TABLE IF NOT EXISTS instagram.profiles (
  `id` BIGINT NOT NULL  PRIMARY KEY,
  `username` VARCHAR(2000) ,
  `name` VARCHAR(2000) ,
  `followings` INT UNSIGNED  ,
  `followers` INT UNSIGNED  ,
  `number_of_posts` INT UNSIGNED,
  `insert_date` DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
"""
location_table_create = """
CREATE TABLE IF NOT EXISTS instagram.locations (
  `id` BIGINT NOT NULL  PRIMARY KEY,
  `name` VARCHAR(2000) ,
  `city` VARCHAR(2000) ,
  `country_code` CHAR(2) ,
  `insert_date` DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
"""
post_type_table_create = """
CREATE TABLE IF NOT EXISTS instagram.post_types (
  `id` TINYINT NOT NULL  PRIMARY KEY,
  `name` VARCHAR(2000) ,
  `insert_date` DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
"""

mariadb_tables = {'profiles':profile_table_create,'locations':location_table_create,'post_types':post_type_table_create,'posts':post_table_create}


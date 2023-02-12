#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 25 20:37:40 2023

@author: anvaari
"""

post_table_create = """
CREATE TABLE IF NOT EXISTS posts
(

    `id` Int64,

    `profile_id` Int64,

    `location_id` Int64,

    `date` DateTime,

    `post_type` UInt8,

    `like` Nullable(UInt32),

    `comments` Nullable(UInt32),

    `insert_date` DateTime,

)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 8192;  
"""

profile_table_create = """
CREATE TABLE IF NOT EXISTS profiles
(

    `id` Int64,

    `username` Nullable(String),

    `name` Nullable(String),

    `followings` Nullable(UInt32),

    `followers` Nullable(UInt32),

    `number_of_posts` Nullable(UInt32),

    `insert_date` DateTime,

)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 8192;  
"""
location_table_create = """
CREATE TABLE IF NOT EXISTS locations
(

    `id` Int64,

    `name` Nullable(String),

    `city` Nullable(String),

    `country_code` Nullable(FixedString(2)),

    `insert_date` DateTime,

)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 8192;  
"""
post_type_table_create = """
CREATE TABLE IF NOT EXISTS post_types
(

    `id` UInt8,

    `name` Nullable(String),

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
  `profile_id` BIGINT  REFERENCES profiles(id),
  `location_id` BIGINT  REFERENCES locations(id),
  `date` DATETIME ,
  `post_type` TINYINT  REFERENCES post_types(id),
  `like` INT UNSIGNED ,
  `comments` INT UNSIGNED ,
  `insert_date` DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

"""

profile_table_create = """

CREATE TABLE IF NOT EXISTS instagram.profiles (
  `id` BIGINT   PRIMARY KEY,
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


analytic_query_1 = """
select 
	max(name) as name,
    sum(`like`)  as like_sum,
    sum(comments) as comments_count
from instagram.posts p inner join instagram.post_types pt on p.post_type=pt.id  
group by post_type
"""

analytic_query_2 = """
select 
	profile_id,
	argMax(location_id,location_count) as most_used_location_id,
	argMax(name,location_count) as most_used_location_name,
    max(location_count) as number_of_posts_with_location
from(
	select 
	    location_id,
	    groupArray(name)[1] as name,
	    profile_id,
	    count(id) as location_count
	from instagram.posts p
	    inner join instagram.locations  l on l.id=p.location_id
	where profile_id >=0 and location_id >=0
	group by location_id,profile_id
)
group by profile_id
    """
    
analytic_query_3 = """
select 
    profile_id,
    groupArray(name)[1] as name,
    groupArray(followers)[1] as followers,
    groupArray(number_of_posts)[1] as number_of_posts,
    avg(engagement_rate) as mean_engagement_rate
from(
     select 
         profile_id,
         name,
         followers,
         number_of_posts,
         ((like + comments) / followers) as engagement_rate
     from instagram.posts post
         inner join instagram.profiles prof on post.profile_id = prof.id
     where like is not null and comments is not null and followers is not null and followers != 0
     )
group by profile_id
order by mean_engagement_rate desc
"""

clickhouse_queries= {'post_type':analytic_query_1,
                     'profile_location':analytic_query_2,
                     'engagement_rate':analytic_query_3}

analytic_query_1 = """
select 
	max(name) as name,
    sum(`like`)  as like_sum,
    sum(comments) as comments_count
from instagram.posts p inner join instagram.post_types pt on p.post_type=pt.id  
group by post_type
"""

analytic_query_2 = """
select 
	normal.profile_id,
	normal.location_id,
	normal.name,
    normal.location_count
from(
	select 
	    location_id,
	    max(name) as name,
	    profile_id,
	    count(l.id) as location_count
	from instagram.posts p
	    inner join instagram.locations  l on l.id=p.location_id
	where profile_id >=0 and location_id >=0
	group by location_id,profile_id
) as normal
inner join 
(select 
	b.profile_id as b_p,
    max(b.location_count) as max_loc
from(
	select 
	    profile_id,
	    count(l.id) as location_count
	from instagram.posts p
	    inner join instagram.locations  l on l.id=p.location_id
	where profile_id >=0 and location_id >=0
	group by location_id,profile_id
) as b
group by profile_id ) as max_loc_q
where normal.location_count = max_loc_q.max_loc 
    """
    
analytic_query_3 = """
select 
    profile_id,
    max(name) as name,
    max(followers) as followers,
    max(number_of_posts) as number_of_posts,
    avg(engagement_rate) as mean_engagement_rate
from(
     select 
         profile_id,
         name,
         followers,
         number_of_posts,
         ((`like` + comments) / followers) as engagement_rate
     from instagram.posts post
         inner join instagram.profiles prof on post.profile_id = prof.id
     where `like` is not null and comments is not null and followers is not null and followers != 0
     ) as eng_q
group by profile_id
order by mean_engagement_rate desc
"""

mariadb_queries= {'post_type':analytic_query_1,
                     'profile_location':analytic_query_2,
                     'engagement_rate':analytic_query_3}

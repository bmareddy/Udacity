import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('C:\\Users\\madhu\\Documents\\git\\Udacity\\DEND\\CloudDW\\Data_Warehouse_Project\\dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS events_staging;"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging;"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays"
user_table_drop = "DROP TABLE IF EXISTS dim_users"
song_table_drop = "DROP TABLE IF EXISTS dim_songs"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS events_staging (
    artist VARCHAR(255),
    auth VARCHAR(255),
    firstName VARCHAR(30),
    gender VARCHAR(1),
    itemInSession INTEGER,
    lastName VARCHAR(30),
    length FLOAT,
    level VARCHAR(30),
    location VARCHAR(255),
    method VARCHAR(30),
    page VARCHAR(30),
    registration VARCHAR(255),
    sessionId INTEGER,
    song VARCHAR(255),
    status VARCHAR(30),
    ts VARCHAR(30),
    userAgent VARCHAR(255),
    userId INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS songs_staging (
    num_songs SMALLINT,
    artist_id VARCHAR(30),
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(255),
    artist_name VARCHAR(255),
    song_id VARCHAR(30),
    title VARCHAR(255),
    duration FLOAT,
    year SMALLINT
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplays (
songplay_id BIGINT NOT NULL IDENTITY(1,1),
start_time TIMESTAMP NOT NULL DISTKEY SORTKEY,
user_id INT NOT NULL,
level VARCHAR(30),
song_id VARCHAR(30) NOT NULL,
artist_id VARCHAR(30) NOT NULL,
session_id INT,
location VARCHAR(255),
user_agent VARCHAR(255)
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_users (
user_id INT NOT NULL SORTKEY,
first_name VARCHAR(30),
last_name VARCHAR(30),
gender VARCHAR(1),
level VARCHAR(30),
as_of_timestamp TIMESTAMP
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_songs (
song_id VARCHAR(30) NOT NULL SORTKEY,
title VARCHAR(255),
artist_id VARCHAR(30) NOT NULL,
year SMALLINT,
duration FLOAT
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artists (
artist_id VARCHAR(30) NOT NULL SORTKEY,
name VARCHAR(255),
location VARCHAR(255),
latitude FLOAT,
longitude FLOAT
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time (
start_time TIMESTAMP NOT NULL SORTKEY,
hour SMALLINT,
day SMALLINT,
week SMALLINT,
month SMALLINT,
year SMALLINT,
weekday SMALLINT
)
""")

# LOAD STAGING TABLES
staging_events_copy = ("""
COPY events_staging
FROM {0}
IAM_ROLE {1}
FORMAT AS JSON {2};
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY songs_staging
FROM {0}
IAM_ROLE {1}
FORMAT AS JSON 'auto';
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# LOAD DESTINATION FACT AND DIMENSION TABLES FROM STAGING
songplay_table_insert = ("""
INSERT INTO fact_songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + (CAST(es.ts AS BIGINT)/1000) * INTERVAL '1 second',
es.userId,
es.level,
ISNULL(ds.song_id, 'UNKNOWN'),
ISNULL(ds.artist_id, 'UNKNOWN'),
es.sessionId,
es.location,
es.userAgent
FROM events_staging es
LEFT JOIN dim_songs ds
    ON es.song = ds.title
    AND es.length = ds.duration
WHERE es.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO dim_users (user_id, first_name, last_name, gender, level, as_of_timestamp)
SELECT userId, firstName, lastName, gender, level
FROM (
    SELECT userId, firstName, lastName, gender, level, TIMESTAMP 'epoch' + (CAST(ts AS BIGINT)/1000) * INTERVAL '1 second'
    ROW_NUMBER() OVER (PARTITION BY userId ORDER BY CAST(ts AS BIGINT) DESC) as rownum
    FROM events_staging
    WHERE userId IS NOT NULL
) sub
WHERE rownum = 1
""")

song_table_insert = ("""
INSERT INTO dim_songs (song_id, title, artist_id, duration, year)
SELECT song_id, title, artist_id, duration, CASE WHEN year = 0 THEN NULL ELSE year END
FROM songs_staging
""")

artist_table_insert = ("""
INSERT INTO dim_artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, 
CASE WHEN artist_location = '' THEN NULL ELSE artist_location END, 
CASE WHEN artist_latitude = '' THEN NULL ELSE artist_latitude END, 
CASE WHEN artist_longitude = '' THEN NULL ELSE artist_longitude END
FROM songs_staging
""")

time_table_insert = ("""
INSERT INTO dim_time (start_time, hour, day, week, month, year, weekday)
SELECT ts, 
        DATE_PART(hr, ts),
        DATE_PART(d, ts),
        DATE_PART(w, ts),
        DATE_PART(mon, ts),
        DATE_PART(y, ts),
        DATE_PART(weekday, ts)
FROM (
    SELECT DISTINCT TIMESTAMP 'epoch' + (CAST(ts AS BIGINT)/1000) * INTERVAL '1 second' as ts
    FROM events_staging
) sub
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, songplay_table_insert, time_table_insert]

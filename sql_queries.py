import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession int,
    lastName varchar,
    length DOUBLE PRECISION,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration varchar,
    sessionId int,
    song varchar,
    status int,
    ts bigint,
    userAgent varchar,
    userId int,
    diststyle even
);
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs (
    num_songs text,
    artist_id text,
    artist_latitude numeric,
    artist_logitude numeric,
    artist_location varchar,
    song_id int,
    title varchar,
    duration bigint,
    year int
);
""")

songplay_table_create = ("""CREATE TABLE songplays (
    songplay_id IDENTITY(0,1),
    start_time timestamp,
    user_id int,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id int,
    location varchar,
    user_agent varchar,
    PRIMARY KEY ()
);
""")

user_table_create = ("""CREATE TABLE user (
    user_id int,
    fisrt_name varchar,
    last_name varchar,
    gender varchar,
    level varchar,
    PRIMARY KEY (user_id)
    );
""")

song_table_create = ("""CREATE TABLE songs (
    song_id varchar,
    title varchar,
    artist_id varchar,
    year int,
    duration int,
    PRIMARY KEY (song_id)
);
""")

artist_table_create = ("""CREATE TABLE artist (
    artist_id int,
    name varchar,
    location varchar,
    latitude numeric,
    longitude numeric,
    PRIMARY KEY (artist_id)
);
""")

time_table_create = ("""CREATE TABLE time (
    start_time timestamp,
    hour numeric,
    day numeric,
    week numeric,
    month numeric,
    year numeric,
    weekday varchar,
    PRIMARY KEY (start_time)
);
""")

# STAGING TABLES

staging_events_copy = ("copy staging_songs from {} iam_role {} json 'auto'").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'])

staging_songs_copy = ("copy staging_songs from {} iam_role {} json 'auto'").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

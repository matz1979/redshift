import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop =  "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
    artist        varchar,
    auth          varchar,
    firstName     varchar,
    gender        varchar,
    itemInSession integer,
    lastName      varchar,
    length        decimal,
    level         varchar,
    location      varchar,
    method        varchar,
    page          varchar,
    registration  varchar,
    sessionId     integer,
    song          varchar,
    status        integer,
    ts            bigint,
    userAgent     text,
    userId        integer);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs       varchar,
    artist_id       text,
    artist_name     varchar,
    artist_latitude float,
    artist_longitude float,
    artist_location varchar,
    song_id         varchar,
    title           varchar,
    duration        float,
    year            integer);
""")


user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
    user_id integer NOT NULL sortkey distkey PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
    song_id varchar NOT NULL sortkey distkey PRIMARY KEY,
    title varchar,
    artist_id text NOT NULL,
    year integer,
    duration float);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist (
    artist_id text sortkey distkey PRIMARY KEY NOT NULL,
    name varchar,
    location varchar,
    latitude float,
    longitude float);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
    start_time timestamp NOT NULL distkey PRIMARY KEY,
    hour integer,
    day integer,
    week integer,
    month integer,
    year integer,
    weekday integer);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
    songplay_id integer IDENTITY(0,1) distkey PRIMARY KEY,
    start_time bigint NOT NULL sortkey REFERENCES time,
    user_id integer NOT NULL REFERENCES users,
    level varchar,
    song_id varchar NOT NULL REFERENCES songs,
    artist_id text NOT NULL REFERENCES artist,
    session_id integer,
    location varchar,
    user_agent varchar);
""")


# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {} 
    iam_role {}
    json {}
    REGION 'us-west-2';
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])
                        

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    iam_role {}
    REGION 'us-west-2'
    COMPUPDATE OFF
    json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])
                         

# FINAL TABLES

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT
                         userId AS user_id, 
                         firstname AS first_name, 
                         lastname AS last_name, 
                         gender, 
                         level 
                        FROM staging_events
                        WHERE page = 'NextSong';
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT
                         song_id,
                         title,
                         artist_id,
                         year,
                         duration
                        FROM staging_songs;
""")

artist_table_insert = ("""INSERT INTO artist (artist_id, name, location, latitude, longitude)
                         SELECT
                         artist_id, 
                         artist_name as name, 
                         artist_location as location, 
                         artist_latitude as latitude, 
                         artist_longitude as longitude
                        FROM staging_songs
                        ;
""")


songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT
                             event.ts as start_time,
                             event.userId AS user_id,
                             event.level AS level,
                             songs.song_id AS song_id,
                             songs.artist_id AS artist_id,
                             event.sessionId as session_id,
                             event.location as location,
                             event.userAgent AS user_agent
                            FROM staging_events AS event
                            JOIN staging_songs AS songs
                            ON (event.artist = songs.artist_name)
                            AND (event.song = songs.title)
                            AND (event.length = songs.duration)
                            WHERE event.page = 'NextSong';
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT
                         a.start_time,
                         extract (h from a.start_time) AS hour,
                         extract (d from a.start_time) AS day,
                         extract (w from a.start_time) AS week,
                         extract (mon from a.start_time) AS month,
                         extract (y from a.start_time) AS year,
                         extract (weekday from a.start_time) AS weekday
                        FROM 
                        (SELECT TIMESTAMP 'epoch' + start_time/1000 * interval '1 second' AS start_time FROM songplays) a
                        ;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, songplay_table_insert, time_table_insert]

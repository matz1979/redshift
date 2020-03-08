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
    artist_latitude decimal,
    artist_logitude decimal,
    artist_location varchar,
    song_id         varchar,
    title           varchar,
    duration        float,
    year            integer);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay (
    songplay_id integer IDENTITY(0,1) distkey,
    start_time bigint sortkey,
    user_id integer,
    level varchar,
    song_id varchar,
    artist_id text,
    session_id integer,
    location varchar,
    user_agent varchar);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
    user_id integer sortkey distkey,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
    song_id varchar sortkey distkey,
    title varchar,
    artist_id text,
    year integer,
    duration float);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist (
    artist_id text sortkey distkey,
    name varchar,
    location varchar,
    latitude decimal,
    longitude decimal);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
    start_time timestamp distkey,
    hour numeric,
    day numeric,
    week numeric,
    month numeric,
    year numeric,
    weekday numeric);
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
                         firstName AS first_name,
                         lastName AS last_name,
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
                         ste.artist,
                         artist_location,
                         artist_latitude,
                         artist_logitude
                        FROM staging_songs sts
                        JOIN staging_events ste
                        ON (ste.location = sts.artist_location);
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT ts,
                         TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ' as start_time,
                         extract (h from start_time) AS hour,
                         extract (d from start_time) AS day,
                         extract (w from start_time) AS week,
                         extract (mon from start_time) AS month,
                         extract (y from start_time) AS year,
                         extract (weekday from start_time) AS weekday
                        FROM staging_events
                        ;
""")

songplay_table_insert = ("""INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT
                             ste.ts as start_time,
                             ste.userId AS user_id,
                             ste.level,
                             sts.song_id,
                             sts.artist_id,
                             ste.sessionId,
                             ste.location,
                             ste.userAgent
                            FROM staging_events ste, staging_songs sts
                            WHERE ste.page = 'NextSong'
                            AND ste.song = sts.title
                            AND ste.artist = sts.artist_name
                            AND ste.length = sts.duration;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

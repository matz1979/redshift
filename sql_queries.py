import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop =  "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop =   "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop =        "DROP TABLE IF EXISTS songplays"
user_table_drop =            "DROP TABLE IF EXISTS user"
song_table_drop =            "DROP TABLE IF EXISTS songs"
artist_table_drop =          "DROP TABLE IF EXISTS artist"
time_table_drop =            "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
    artist        varchar encode text255,
    auth          varchar encode text255,
    firstName     varchar,
    gender        varchar,
    itemInSession integer,
    lastName      varchar,
    length        decimal,
    level         varchar,
    location      varchar encode text255,
    method        varchar,
    page          varchar,
    registration  varchar,
    sessionId     integer,
    song          varchar,
    status        integer,
    ts            bigint,
    userAgent     varchar encode text255,
    userId        integer,
    diststyle     auto
);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs       varchar,
    artist_id       varchar,
    artist_latitude decimal,
    artist_logitude decimal,
    artist_location varchar,
    song_id         integer,
    title           varchar,
    duration        bigint,
    year            integer,
    diststyle       auto
);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
    songplay_id         IDENTITY(0,1),
    start_time          timestamp REFERENCES time,
    user_id             integer REFERENCES user,
    level               varchar,
    song_id             varchar REFERENCES song,
    artist_id           varchar REFERENCES artist,
    session_id          integer,
    location            varchar,
    user_agent          varchar,
    diststyle           auto,
    PRIMARY KEY (songplay_id),
    DISTKEY (songplay_id),
    SORTKEY (start_time)
);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS user (
    user_id         integer,
    first_name      varchar,
    last_name       varchar,
    gender          varchar,
    level           varchar,
    PRIMARY KEY (user_id),
    SORTKEY (user_id)
    );
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
    song_id         varchar,
    title           varchar,
    artist_id       varchar,
    year            integer,
    duration        decimal,
    PRIMARY KEY (song_id),
    SORTKEY (song_id)
);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist (
    artist_id       integer,
    name            varchar,
    location        varchar,
    latitude        decimal,
    longitude       decimal,
    PRIMARY KEY (artist_id),
    SORTKEY (artist_id)
);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
    start_time      timestamp,
    hour            numeric,
    day             numeric,
    week            numeric,
    month           numeric,
    year            numeric,
    weekday         varchar,
    PRIMARY KEY (start_time),
    SORTKEY (start_time)
);
""")

# STAGING TABLES

staging_events_copy = ("copy staging_songs from {} iam_role {} json 'auto'").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("copy staging_songs from {} iam_role {} json 'auto'").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay (
                            SELECT   
                             events.ts,
                             events.userId,
                             events.level,
                             songs.song_id,
                             songs.artist_id,
                             events.sessionId,
                             events.location,
                             events.userAgent
                            FROM staging_events AS events
                            JOIN staging_songs AS songs
                            ON (events.artist = songs.artist_name)
                            AND (events.song = songs.title)
                            AND (events.length = songs.duration)
                            WHERE events.page = 'NextSong';
                             
);
""")

user_table_insert = ("""INSERT INTO user (
                        SELECT DISTINCT 
                         userId AS user_id, 
                         firstName AS first_name, 
                         lastName AS last_name, 
                         gender, 
                         level 
                        FROM staging_events
                        WHERE page = "NextSong"
);
""")

song_table_insert = ("""INSERT INTO song (
                        SELECT DISTINCT
                         song_id,
                         title,
                         artist_id,
                         year,
                         duration
                        FORM staging_songs
);
""")

artist_table_insert = ("""INSERT INTO artist (
                         SELECT DISTINCT 
                         artist_id, 
                         ste.artist, 
                         artist_location, 
                         artist_latitude, 
                         artist_longitude 
                        FROM staging_songs sts
                        JOIN staging_events ste
                        ON (ste.location = sts.artist_location)
);
""")

time_table_insert = ("""INSERT INTO time (
                        SELECT ts
                         extract ('epoch' from timestamp + ts/1000 * interval '1 second' AS start_time,
                         extract (h from ts) AS hour,
                         extract (d from ts) AS day,
                         extract (w from ts) AS week,
                         extract (mon from ts) AS month,
                         extract (y from ts) AS year,
                        CASE 
                            WHEN extract (dow from ts) = 0 then 'Sunday'
                            WHEN extract (dow from ts) = 1 then 'Monday'
                            WHEN extract (dow from ts) = 2 then 'Tuesday'
                            WHEN extract (dow from ts) = 3 then 'Wednesday'
                            WHEN extract (dow from ts) = 4 then 'Thursday'
                            WHEN extract (dow from ts) = 5 then 'Friday'
                            WHEN extract (dow from ts) = 6 then 'Saturday'
                        END AS weekday
                        FROM staging_events
);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

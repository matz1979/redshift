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
    itemInSession integer,
    lastName varchar,
    length decimal,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration varchar,
    sessionId integer,
    song varchar,
    status integer,
    ts bigint,
    userAgent varchar,
    userId integer,
    diststyle even
);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs varchar,
    artist_id varchar,
    artist_latitude decimal,
    artist_logitude decimal,
    artist_location varchar,
    song_id integer,
    title varchar,
    duration bigint,
    year integer,
    diststyle even
);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
    songplay_id IDENTITY(0,1),
    start_time timestamp,
    user_id integer,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id integer,
    location varchar,
    user_agent varchar,
    diststyle even,
    PRIMARY KEY (songplay_id),
    DISTKEY (songplay_id),
    SORTKEY (start_time)
);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS user (
    user_id integer SORTKEY,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar,
    PRIMARY KEY (user_id),
    SORTKEY (user_id)
    );
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
    song_id varchar,
    title varchar,
    artist_id varchar,
    year integer,
    duration decimal,
    PRIMARY KEY (song_id),
    SORTKEY (song_id)
);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist (
    artist_id integer,
    name varchar,
    location varchar,
    latitude decimal,
    longitude decimal,
    PRIMARY KEY (artist_id),
    SORTKEY (artist_id)
);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
    start_time timestamp,
    hour numeric,
    day numeric,
    week numeric,
    month numeric,
    year numeric,
    weekday varchar,
    PRIMARY KEY (start_time),
    SORTKEY (start_time)
);
""")

# STAGING TABLES

staging_events_copy = ("copy staging_songs from {} iam_role {} json 'auto'").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'])

staging_songs_copy = ("copy staging_songs from {} iam_role {} json 'auto'").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay (
                         
);
""")

user_table_insert = ("""INSERT INTO user (
                         SELECT user_id, firstName, lastName, gender, level FROM staging_events
);
""")

song_table_insert = ("""INSERT INTO song (
                         SELECT 
);
""")

artist_table_insert = ("""INSERT INTO artist (
                         SELECT 
);
""")

time_table_insert = ("""INSERT INTO time (
                         SELECT
);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

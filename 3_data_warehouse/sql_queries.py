import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
    artist varchar,
    auth varchar,
    firstName varchar,
    gender character(1),
    itemInSession int,
    lastName varchar,
    length Decimal,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration Decimal,
    sessionId int,
    song varchar,
    status smallint,
    ts bigint,
    userAgent varchar,
    userId int
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs int,
    artist_id varchar(20),
    artist_latitude Decimal,
    artist_longitude Decimal,
    artist_location varchar,
    artist_name varchar,
    song_id varchar(20),
    title varchar,
    duration Decimal,
    year smallint
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
    sp_songplay_id bigint IDENTITY(0,1) PRIMARY KEY,
    sp_start_time bigint NOT NULL,
    sp_user_id int NOT NULL sortkey,
    sp_level varchar,
    sp_song_id varchar,
    sp_artist_id varchar distkey,
    sp_session_id int,
    sp_location varchar,
    sp_user_agent varchar
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
    u_user_id int PRIMARY KEY,
    u_first_name varchar sortkey,
    u_last_name varchar,
    u_gender varchar,
    u_level varchar
    )
    diststyle auto;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
    so_song_id varchar PRIMARY KEY,
    so_title varchar,
    so_artist_id varchar distkey,
    so_year int sortkey,
    so_duration Decimal
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
    a_artist_id varchar PRIMARY KEY distkey,
    a_name varchar sortkey,
    a_location varchar,
    a_latitude Decimal,
    a_longitude Decimal
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
    t_start_time timestamp PRIMARY KEY,
    t_hour int,
    t_day int,
    t_week int,
    t_month int,
    t_year int sortkey,
    t_weekday int
    )
    diststyle auto;
""")

# COPY-QUERIES INTO STAGING TABLES (LOAD STAGING TABLES)

staging_events_copy = ("""
    copy staging_events
    from {}
    iam_role {}
    format as json {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    iam_role {}
    format AS json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# INSERT QUERIES FOR FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(sp_start_time, sp_user_id, sp_level, sp_song_id, sp_artist_id, sp_session_id, sp_location, sp_user_agent)
    SELECT DISTINCT se.ts, se.userId, se.level, so.song_id, so.artist_id, se.sessionId, se.location, se.userAgent
    FROM staging_events as se
    LEFT JOIN staging_songs as so
    ON (se.artist = so.artist_name)
    AND (se.song = so.title)
    AND (se.length = so.duration)
    WHERE se.userId IS NOT NULL
    AND se.ts IS NOT NULL;
""")

user_table_insert = ("""
    INSERT INTO users (u_user_id, u_first_name, u_last_name, u_gender, u_level)
    SELECT DISTINCT userId, firstName, lastName, gender, level
    FROM staging_events
    WHERE userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs(so_song_id, so_title, so_artist_id, so_year, so_duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists(a_artist_id, a_name, a_location, a_latitude, a_longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time(t_start_time, t_hour, t_day, t_week, t_month, t_year, t_weekday)
    SELECT DISTINCT timestamp 'epoch' + ts/1000 * interval '1 second' as newts, extract(hour from newts), extract(day from newts), extract(week from newts), extract(month from newts), extract(year from newts), extract(weekday from newts)
    FROM staging_events
    WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

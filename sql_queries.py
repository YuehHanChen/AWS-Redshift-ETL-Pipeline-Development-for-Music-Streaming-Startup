import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist          TEXT,
        auth            TEXT,
        first_name      TEXT,
        gender          TEXT,
        item_in_session INTEGER,
        last_name       TEXT,
        length          FLOAT4,
        level           TEXT,
        location        TEXT,
        method          TEXT,
        page            TEXT,
        registration    FLOAT8,
        session_id      INTEGER,
        song            TEXT,
        status          INTEGER,
        ts              BIGINT,
        user_agent      TEXT,
        user_id         TEXT);
""")

staging_songs_table_create = ("""
     CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs           INTEGER,
        artist_id           TEXT,
        artist_latitude     REAL,
        artist_longitude    REAL,
        artist_location     TEXT,
        artist_name         TEXT,
        song_id             TEXT,
        title               TEXT,
        duration            FLOAT,
        year                INTEGER );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (
        songplay_id    BIGINT IDENTITY(0,1) PRIMARY KEY,
        start_time     TIMESTAMP REFERENCES time(start_time),
        user_id        TEXT REFERENCES users(user_id),
        level          TEXT,
        song_id        TEXT REFERENCES songs(song_id),
        artist_id      TEXT REFERENCES artists(artist_id),
        session_id     INTEGER,
        location       TEXT,
        user_agent     TEXT
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id     TEXT PRIMARY KEY SORTKEY,
        first_name  TEXT,
        last_name   TEXT,
        gender      TEXT,
        level       TEXT
    ) diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id     TEXT PRIMARY KEY SORTKEY,
        title       TEXT,
        artist_id   TEXT,
        year        SMALLINT,
        duration    FLOAT4
    ) diststyle all;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id   TEXT PRIMARY KEY SORTKEY,
        name        TEXT,
        location    TEXT,
        latitude    FLOAT,
        longitude   FLOAT
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time  TIMESTAMP PRIMARY KEY,
        hour        SMALLINT,
        day         SMALLINT,
        week        SMALLINT,
        month       SMALLINT,
        year        SMALLINT,
        weekday     SMALLINT
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY {} FROM {}
    IAM_ROLE '{}'
    JSON {} region '{}';
""").format('staging_events',
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH'],
    config['CLUSTER']['REGION'])

staging_songs_copy = ("""
    COPY {} FROM {}
    IAM_ROLE '{}'
    JSON 'auto' region '{}';
""").format('staging_songs',
    config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['CLUSTER']['REGION'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT
        TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second'),
        e.user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.session_id,
        e.location,
        e.user_agent
    FROM staging_events e
    LEFT JOIN staging_songs s ON e.song = s.title
        AND e.artist = s.artist_name
        AND ABS(e.length - s.duration) < 2
    WHERE
        e.page = 'NextSong'
""")

user_table_insert = ("""
     INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT user_id, first_name, last_name, gender, level
               FROM staging_events
              WHERE user_id IS NOT NULL 
                AND first_name IS NOT NULL
                AND last_name IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
               FROM staging_songs
              WHERE song_id IS NOT NULL
                AND title IS NOT NULL
                AND duration IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
           FROM staging_songs
          WHERE artist_id IS NOT NULL
            AND artist_name IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT timestamp 'epoch' + ts / 1000 * interval '1 second' AS start_time,
                extract(hour from (timestamp 'epoch' + ts / 1000 * interval '1 second')),
                extract(day from (timestamp 'epoch' + ts / 1000 * interval '1 second')),
                extract(week from (timestamp 'epoch' + ts / 1000 * interval '1 second')),
                extract(month from (timestamp 'epoch' + ts / 1000 * interval '1 second')),
                extract(year from (timestamp 'epoch' + ts / 1000 * interval '1 second')),
                extract(weekday from (timestamp 'epoch' + ts / 1000 * interval '1 second'))
           FROM staging_events
          WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

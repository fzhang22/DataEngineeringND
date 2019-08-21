import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# ACESS INFO FROM CONFIG FILE
arn = config['IAM_ROLE']['ARN']
log_data = config['S3']['LOG_DATA']
log_json_path = config['S3']['LOG_JSONPATH']
song_data = config['S3']['SONG_DATA']

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
       artist         VARCHAR(MAX),
       auth           VARCHAR(MAX),
       firstName      CHAR(50),
       gender         CHAR(1),
       itemInSession  INTEGER,
       lastName       CHAR(50),
       length         DECIMAL,
       level          CHAR(5),
       location       VARCHAR(200),
       method         CHAR(20),
       page           CHAR(20),
       registration   DECIMAL,
       sessionId      INTEGER,
       song           VARCHAR(MAX),
       status         INTEGER,
       ts             BIGINT,
       userAgent      VARCHAR(MAX),
       userId         INTEGER
     )
""")

staging_songs_table_create = ("""
   CREATE TABLE IF NOT EXISTS staging_songs(
       num_songs             INTEGER,
       artist_id             VARCHAR(256),
       artist_latitude       DECIMAL,
       artist_longitude      DECIMAL,
       artist_location       VARCHAR(MAX),
       artist_name           VARCHAR(256),
       song_id               VARCHAR(256),
       title                 VARCHAR(MAX),
       duration              DECIMAL,
       year                  INTEGER
       )
""")

songplay_table_create = ("""
   CREATE TABLE IF NOT EXISTS songplay(
       songplay_id     INTEGER IDENTITY(1,1) PRIMARY KEY,
       start_time      TIMESTAMP,
       user_id         INTEGER NOT NULL,
       level           CHAR(5),    
       song_id         CHAR(80),
       artist_id       VARCHAR(120),
       session_id      INTEGER,
       location        VARCHAR(256),
       user_agent      VARCHAR(MAX)
       )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id        INTEGER PRIMARY KEY,
        first_name     CHAR(20) NOT NULL,
        last_name      CHAR(20) NOT NULL,
        gender         CHAR(1),
        level          CHAR(5)
        )
""")

song_table_create = ("""
   CREATE TABLE IF NOT EXISTS songs(
       song_id         CHAR(80) PRIMARY KEY,
       title           VARCHAR(256) NOT NULL,
       artist_id       VARCHAR(256) NOT NULL,
       year            INTEGER,
       duration        DECIMAL
       )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
       artist_id       VARCHAR(120) PRIMARY KEY,
       name            VARCHAR(256) NOT NULL,
       location        VARCHAR(256),
       lattitude       DECIMAL,
       longitude       DECIMAL
       )   
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
       start_time     TIMESTAMP PRIMARY KEY,
       hour           INTEGER,
       day            INTEGER,
       week           INTEGER,
       month          INTEGER,
       year           INTEGER,
       weekday        INTEGER
       )
""")

# STAGING TABLES
# Load from JSON Data Using a JSONPaths file
staging_events_copy = ("""
   copy staging_events
   from {}
   iam_role {}
   json {}
""").format(log_data, arn, log_json_path)

# Load from JSON Data Using the 'auto' Option
staging_songs_copy = ("""
    copy staging_songs
    from {}
    iam_role {}
    json 'auto'
""").format(song_data, arn)

# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplay(start_time, 
                         user_id, 
                         level, 
                         song_id, 
                         artist_id, 
                         session_id, 
                         location, 
                         user_agent)
    SELECT timestamp 'epoch' + se.ts * interval '1 second' as start_time, 
    se.userId, 
    se.level, 
    ss.song_id, 
    ss.artist_id, 
    se.sessionId, 
    se.location, 
    se.userAgent
    FROM staging_events se, staging_songs ss
    WHERE se.page = 'NextSong' AND 
    se.artist = ss.artist_name AND
    se.length = ss.duration AND 
    se.song = ss.title
    
""")

user_table_insert = ("""
    INSERT INTO users(user_id,
                      first_name,
                      last_name,
                      gender,
                      level)
    SELECT DISTINCT userId, firstName, lastName, gender, level
    FROM staging_events
    WHERE page = 'NextSong'
    
""")
      
song_table_insert = ("""
    INSERT INTO songs(song_id,
                      title,
                      artist_id,
                      year,
                      duration)
    SELECT song_id, title, artist_id, year, duration
    FROM staging_songs

""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id,
                        name,
                        location,
                        lattitude,
                        longitude)
    SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    
""")

time_table_insert = ("""
    INSERT INTO time(start_time,
                     hour,
                     day,
                     week,
                     month,
                     year,
                     weekday)
    SELECT start_time,
           extract(hour from start_time),
           extract(day from start_time),
           extract(week from start_time),
           extract(month from start_time),
           extract(year from start_time),
           extract(dayofweek from start_time)
    FROM songplay
    
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

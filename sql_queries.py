import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events
    (
		artist VARCHAR,
		auth VARCHAR,
		firstName VARCHAR,
		gender VARCHAR,
		itemInSession INT,
		lastName VARCHAR,
		length FLOAT,
		level VARCHAR,
		location VARCHAR,
		method VARCHAR,
		page VARCHAR,
		registration FLOAT,
		sessionId INT,
		song VARCHAR distkey,
		status INT,
		ts BIGINT,
		userAgent VARCHAR,
		userId INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs 
    (
		num_songs VARCHAR,
		artist_id VARCHAR,
		artist_latitude DECIMAL(9,6),
		artist_longitude DECIMAL(10,6),
		artist_location VARCHAR,
		artist_name VARCHAR,
		song_id VARCHAR,
		title VARCHAR,
		duration FLOAT,
		year SMALLINT
    );
""")

songplay_table_create = (""" 
    CREATE TABLE songplays 
    (
        songplay_id INT IDENTITY(0,1) PRIMARY KEY,
        start_time BIGINT NOT NULL,
        user_id INT NOT NULL,
        level VARCHAR NOT NULL,
        song_id VARCHAR distkey,
        artist_id VARCHAR,
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE users 
    (
        user_id INT PRIMARY KEY,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR,
        gender VARCHAR(2) NOT NULL,
        level VARCHAR NOT NULL
    )diststyle all;
""")

song_table_create = (""" 
    CREATE TABLE songs 
    (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        year SMALLINT sortkey,
        duration DECIMAL NOT NULL
    )diststyle all;
""")

artist_table_create = (""" 
    CREATE TABLE artists 
    (
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude DECIMAL(9,6),
        longitude DECIMAL(10,6)
    )diststyle all;
""")

time_table_create = (""" 
    CREATE TABLE time 
    (
        start_time BIGINT PRIMARY KEY sortkey,
        hour INT NOT NULL,
        day INT NOT NULL,
        week INT NOT NULL,
        month INT NOT NULL,
        year INT NOT NULL,
        weekday INT NOT NULL
    )diststyle all; 
""")


# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {} 
    iam_role {} region 'us-west-2' json {} ;
""").format(
    config.get("S3", "LOG_DATA"), 
    config.get("IAM_ROLE", "ARN"), 
    config.get("S3", "LOG_JSONPATH")
)

staging_songs_copy = ("""
    COPY staging_songs FROM {} 
    iam_role {} region 'us-west-2' json 'auto';
""").format(
    config.get("S3", "SONG_DATA"), 
    config.get("IAM_ROLE", "ARN")
)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays
    (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT 
        se.start_time,
        se.user_id,
        se.level,
        s.song_id,
        a.artist_id,
        se.session_id,
        se.location,
        se.user_agent
    FROM    
        (SELECT 
            ts AS start_time, 
            userId AS user_id, 
            level, 
            song AS song_title, 
            length, 
            artist AS artist_name, 
            sessionId AS session_id, 
            location,
            userAgent AS user_agent
        FROM staging_events
        WHERE userId IS NOT NULL)se
        LEFT OUTER JOIN
        (SELECT
            song_id,
            title,
            artist_id,
            duration
        FROM songs)s
        ON se.song_title=s.title 
        AND se.length=s.duration
        LEFT OUTER JOIN
        (SELECT
            artist_id,
            name
        FROM artists)a
        ON a.artist_id=s.artist_id
        AND a.name=se.artist_name;
""")

user_table_insert = ("""
	INSERT INTO
		users
	SELECT DISTINCT
		userId,
		firstName,
		lastName,
		gender,
		level
	FROM
		staging_events
    WHERE 
        userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO
        songs
    SELECT
        song_id,
        title,
        artist_id,
        YEAR,
        duration
    FROM
        staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO
        artists
    SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM
        staging_songs;
""")

time_table_insert = ("""
	INSERT INTO
		time
	SELECT
		ts ,
		EXTRACT(HOUR FROM start_time) ,
		EXTRACT(DAY FROM start_time) ,
		EXTRACT(WEEK FROM start_time) ,
		EXTRACT(MONTH FROM start_time) ,
		EXTRACT(YEAR FROM start_time) ,
		EXTRACT(DOW FROM start_time)
	FROM
	(
	SELECT DISTINCT
		TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
        ts
	FROM staging_events 
	) t;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [ user_table_insert, song_table_insert, artist_table_insert, songplay_table_insert, time_table_insert]

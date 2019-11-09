## Background Information
The music streaming startup, Sparkify, has grown their user base and song database and currently have their data in S3, in a directory of containing files of JSON logs of user activity on the app, and another directory with JSON metadata about the songs on their app. 

## Purpose 
Converting song and log data files to properly formatted and segregated tables in a Star Schema that would allow easy and instant query over the data for fetching meaningful results and insights. AWS Redshift is used for data ingestion and ETL transformation.
***

## Running Instruction
Update `dwh.cfg` file with Redshift cluster details.  
Execute `python create_tables.py` for creating staging, fact and dimension tables.  
Execute `python etl.py` for loading data to staging tables and then to remaining Fact and Dimension Tables.
***

## Files
<ul>
    <li><b>Dashboard.ipynb -</b> Dashboard for running Ad-Hoc or Analytical queries after data injestion.</li>
    <li><b>create_tables.py -</b> Python Script for dropping and creating tables.</li>
    <li><b>dwh.cfg -</b> Configuration file, containing connection details and data location.</li>
    <li><b>etl.py -</b> Python Script for ingesting data.</li>
    <li><b>sql_queries.py -</b> All the PostgreSQL queries used in the project at one place.</li>
</ul>

***

## Data Ingestion
<b>Song File - </b> The data within these JSON files is loaded into `staging_songs` without any filteration or modification and then this staging table is used, to populate `songs` and `artists` tables.

<b>Log File - </b> Just as song data, Log File is loaded from JSON Files onto `staging_events` and from this staging table all the other tables(`users`, `songplays`, `time`) are populated. 

***

## Dependencies
* psycopg2
* sconfigparser

***

## Additional Design Information

#### Fact Table
<b>songplays -</b> records in log data associated with song plays
+ songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    
        
#### Dimension Tables
<b>users -</b> users in the app
+ user_id, first_name, last_name, gender, level

<b>songs -</b> songs in music database
+ song_id, title, artist_id, year, duration

<b>artists -</b> artists in music database
+ artist_id, name, location, lattitude, longitude

<b>time -</b> timestamps of records in songplays broken down into specific units
+ start_time, hour, day, week, month, year, weekday


#### Stagging Tables

<b>staging_events -</b> Log data files
+ event_id, artist, auth, first_name, gender, item_session, last_name, length, level, location, method, page, registration, session_id, song, status, ts, user_agent, user_id

<b>staging_songs -</b> Song metadata data files 
+ num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year

***

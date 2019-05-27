## Data Warehousing using AWS S3 and Redshift

This project ingests data from two different types of JSON files `song_data` and `log_data` and loads the data into a fact table and a set of dimension tables, as described below.

### Source
#### Song Data
Song data describes the metadata of a song such as title, artist and duration. Each song is organized into a file

#### Log Data
Log data constains a record for each play of a song and contains details about what song, when it was played, who played it and how it was played

### Destination
The destination is an Amazon Redshift Cluster which contains a fact table `fact_songplays` and a set of dimension tables `dim_songs`, `dim_artists`, `dim_users`, `dim_time` organized in a star schema model

### ETL Approach
Launch a Redshift cluster and grant appropriate permissions through an IAM role. Ensure that the cluster is accessible to the network and to the required users. Create a fresh database called `udacity` and a user `student`. This database will host the required destination tables of the datawarehouse.

#### Song Data
1. Insert the song data into `songs_staging` table using `COPY` command
6. Perform sanity checks and validations
7. Using `songs_staging` as source, insert into `dim_songs` and `dim_artists` tables

#### Log Data
1. Insert the log data into `events_staging` table using `COPY` command
2. Using `events_staging` as source, load `dim_time` table. Transform `ts` column into `TIMESTAMP` data type and derive the datetime attributes such as `hour`, `day`, `week` and `weekday`
3. Using `events_staging` as source, load `dim_users` table. Rank all the records with in a given user by the descending order of `ts` and filter to records only with `rank = 1`. This will ensure that only the latest record for the user is inserted into the `dim_users` table which ensure the latest value of `level` is captured.
4. Using `events_staging` as source, filter to rows where `page == 'NextSong'` and insert into `fact_songplays` while performing a LEFT JOIN on `dim_songs` table to extract `song_id` and `artist_id` columns. If the corresponding `song_id` and `artist_id` are not found, use `UNKNOWN`.

### Run Instructions
From the shell:
1. Ensure the Redshift cluster is active and the credentials located in `dwh.cfg` are accurate
2. Run `create_tables.py`
3. Run `etl.py`

### Notes
Don't forget to throughly validate the data once the staging tables are loaded.
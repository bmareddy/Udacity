## Data Lakes using AWS S3 and Apache Spark

This project ingests data from two different types of JSON files `song_data` and `log_data` from S3 and writes the data back to S3 as set of fact and dimension files, as described below.

### Source
#### Song Data
Song data describes the metadata of a song such as title, artist and duration. Each song is organized into a file

#### Log Data
Log data constains a record for each play of a song and contains details about what song, when it was played, who played it and how it was played

### Destination
The destination is an S3 bucket which contains a parquet file `songplays` for fact and a set of dimension files `songs`, `artists`, `users`, `time` also as parquet files. `songplays` and `time` files are partitioned by `year` and `month`. `songs` file is partitioned by `year` and `artist_id`

### ETL Approach
Create a user and grant appropriate permissions for reading and writing from Amazon S3 resource. Create a bucket called `udacity-dend-datalakes`. This bucket will host the required output files of the datalake.

#### Song Data
1. Read song data from `s3a://udacity-dend/song_data`
2. Extract columns required for `songs` and `artists` into seprate dataframes
3. Write `songs` dataframe as a parquet file by partitioning on `year` and `artist_id` 
4. Add `row_number` to `artists` dataframe by partitioning on `artist_id` and sorting on `year desc`. Then, filter to keep only records with `rownum == 1`. This will ensure duplicate records of artists are eliminated and only record corresponding to the latest year for each artist is kept.
5. Write `artists` dataframe as a parquet file


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
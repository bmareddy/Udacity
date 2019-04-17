## Data Modeling using Postgres

This project ingests data from two different types of JSON files `song_data` and `log_data` and loads the data into a fact table and a set of dimension tables, as described below.

### Source
#### Song Data
Song data describes the metadata of a song such as title, artist and duration. Each song is organized into a file

#### Log Data
Log data constains a record for each play of a song and contains details about what song, when it was played, who played it and how it was played

### Destination
The destination is a Postgres DB which contains a fact table `songplays` and a set of dimension tables `songs`, `artists`, `users`, `time` organized in a star schema model

### ETL Approach
Create a fresh database called `sparkifydb` and create all the required destination tables.

#### Song Data
1. Get a list of all `song_data` files
2. Iterate through each file and peform the following steps
3. Read the json file into a `DataFrame`
4. select `song_id`, `title`, `artist_id`, `year`, and `duration` column in to a separate data frame
5. Insert the rows from this data frame into `songs` table
6. select `artist_id`, `artist_name`, `artist_location`, `artist_latitude` and `artist_longitude` columns into a separate data frame
7. Insert the rows from this data frame into `artists` table

#### Log Data
1. Get a list of all `song_data` files
2. Iterate through each file and peform the following steps
3. Read the json file into a `DataFrame`
4. Filter to only the rows where `page == 'NextSong'`
5. Copy the data into `songplays_log` table
6. Extract the `ts` column into a separate dataframe and convert it to `datatime` data type
7. Derive datetime attributes such as `hour`, `day`, `week` and `weekday` into a list
8. Insert the time data frame into `time` dimension table
10. select `user_id`, `firstName`, `lastName`, `gender`, and `level` column in to a separate data frame
11. Insert the rows from this data frame into `users` table
12. Add the converted time data frame to the original `DataFrame` in step 3
13. Iterate through each row and
14. Extract the corresponding to `song_id` and `artist_id` values for the `song` and `artist` values of this row
15. Insert the row in to `songplays` table

### Run Instructions
From the shell:
1. Run `create_tables.py`
2. Run `etl.py`

### Notes
Please go through `validations.ipynb` notebook from a high level description of the data
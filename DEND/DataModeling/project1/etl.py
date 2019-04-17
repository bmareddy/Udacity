import os, io
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    cur: postgres connection cursor
    filepath: string that specifies the fully qualified path of the input file
    This function reads the give file into a dataframe and inserts into songs and artists dimension tables
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = [list(row) for row in df[['song_id','title','artist_id','year','duration']].itertuples(index=False)]
    for song in song_data:
        cur.execute(song_table_insert, song)
    
    # insert artist record
    artist_data = [list(row) for row in df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].itertuples(index=False)]
    for artist in artist_data:
        cur.execute(artist_table_insert, artist)


def process_log_file(cur, filepath): 
    """
    cur: postgres connection cursor
    filepath: string that specifies the fully qualified path of the input file
    This function reads the give file into a dataframe and does the following:
    1. inserts into a log table that keeps the data in the raw unprocessed format
    2. Extracts timestamp field, computes time attributes and inserts into time dimension
    3. Extracts user data and inserts into user dimension table
    4. Extracts the relevant id fields from all the dimensions, combines it with measures and inserts into songplays fact table
    """    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # load unprocessed log data into songplays_log table
    f = io.StringIO()
    df.to_csv(f, index=False, header=False, sep='|')
    f.seek(0)
    cur.copy_from(f, 'songplays_log', sep='|')
    
    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = list(zip(list(t.values), list(t.dt.hour), list(t.dt.day), list(t.dt.week), list(t.dt.month), list(t.dt.year), list(t.dt.weekday)))
    column_labels = ['start_time','hour','day','week','month','year','weekday']
    time_df = pd.DataFrame(time_data,columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    df["converted_start_time"] = t
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        results = cur.execute(song_select, (row.song, row.artist, row.length))
        songid, artistid = results if results else 'UNKNOWN SONG', 'UNKNOWN ARTIST'

        # insert songplay record
        songplay_data = [row.converted_start_time, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    cur: postgres connection cursor
    conn: postgres connection
    filepath: string that specifies the fully qualified path of the input file 
    func: function that must be applied on a give file at filepath
    This function is a wrapper that iterates over a directory of files and applies the approriate processing function
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i+1, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
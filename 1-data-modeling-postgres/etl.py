import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Extracts one json file from the filepath (`song_data`).
    
    - Loads the relevant columns into the `songs` and `artists` tables using the `song_table_insert` and `artist_table_insert`, respectively.
    
    - Note: we assume that the each json file from the `song_data` contains only one record.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[["song_id", "title", "artist_id", "year", "duration"]].values[0])
    
    try:
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as e:
        print('Error: Inserting rows')
        print(e)
        
    # insert artist record
    artist_data = list(df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0])
    
    try:
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.Error as e:
        print('Error: Inserting rows')
        print(e)
    
def process_log_file(cur, filepath):
    """
    - Extracts and processes data from the filepath (`log_data`).
    
    - Loads the relevant columns to the `time`, `users` and `songplays` tables.
    
    - Uses the `time_table_insert`, `user_table_insert`  and `songplay_table_insert` queries, respectively.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms', origin='unix')
    
    # insert time data records
    time_data = [df.ts, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['start_time', 'hour', 'day', 'weekOfYear', 'month', 'year', 'weekDay']
    time_df = pd.DataFrame()

    for i in range(len(column_labels)):
        time_df[column_labels[i]] = time_data[i]

    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except psycopg2.Error as e:
            print('Error: Inserting rows')
            print(e)

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row)
        except psycopg2.Error as e:
            print('Error: Inserting rows')
            print(e)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        try:
            cur.execute(song_select, (row.song, row.artist, row.length))
        except psycopg2.Error as e:
            print('Error: Selecting songs')
            print(e)
        
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as e:
            print('Error: Inserting rows')
            print(e)


def process_data(cur, conn, filepath, func):
    """
    - Reads all filepaths matching a `*.json` filetype.
    
    - Iterates over each filepath and calls a function `func` on each iteration.
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
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - Connects with the sparkify database and gets a cursor to it.  
    
    - Processes both the `song_data` and the `log_data` using `process_data` and their respective ETL-functions.
    
    - Finally, closes the connection. 
    """
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()
    except psycopg2.Error as e:
        print('Error: Connecting to the database')
        print(e)
    
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()

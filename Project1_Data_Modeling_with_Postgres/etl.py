import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
"""
Description: This function can be used to read the file in the filepath (data/song_data)
to get the song and its information to populate in the songs and artirst tables

Arguments:
    cur: the cursor object. 
    filepath: song_data file path. 

Returns:
    None
"""
    
    # open song file
    df = pd.read_json(filepath, typ='series')

    # insert song record
    song_data = df[['song_id','title','artist_id', 'year', 'duration']]
  
    # check for song_id duplicates
    cur.execute(song_select, (song_data[['song_id']]))
    results = cur.fetchone()
    
    # if no duplicates then insert new records
    if results[0] == 0:
        cur.execute(song_table_insert, song_data)
        
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location', 'artist_latitude', 'artist_longitude']]
    
    # check for artist_id duplicates
    cur.execute(artist_select, (artist_data[['artist_id']]))
    results = cur.fetchone()
   
    if results[0] == 0:
        cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
"""
Description: This function can be used to read the file in the filepath (data/log_data)
to get the user and time info and used to populate the users and time dim tables.

Arguments:
    cur: the cursor object. 
    filepath: log data file path. 

Returns:
    None
"""

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t=pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records to dataframe 
    time_data = [t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year,t.dt.weekday]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']

    dictionary = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame.from_dict(dictionary)

    
    for i, row in time_df.iterrows():
    cur.execute(time_table_insert, list(row))
        

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        # check for user_id duplicates
        cur.execute(user_select, (str(row.userId),))
        results = cur.fetchone()
        
        if results[0] == 0:
            cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select_by_song_id_artist_id, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
      
    
def process_data(cur, conn, filepath, func):
"""
Description: This function can be used to read all files machting extension from directory, get the number of files found
iterate over files and process

Arguments:
    cur: the cursor object.
    conn: connection string
    filepath: file path of all files
    func: iterate the files, process and commit

Returns:
    None
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
 
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
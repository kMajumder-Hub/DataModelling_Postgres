import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import datetime

"""
    This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    
"""

def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[["song_id", "title", "artist_id", "year", "duration"]].values.flatten())
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values.flatten())
    cur.execute(artist_table_insert, artist_data)


"""
    This procedure processes a log file whose filepath has been provided as an arugment.
    It extracts the log information in order to store it into the time, users, and sonlplays table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the log file
    
"""

def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = df["ts"].apply(pd.to_datetime)
    
    # insert time data records
    start_time = t
    hour = t.dt.hour
    day = t.dt.day
    week = t.dt.isocalendar().week
    month = t.dt.month
    year = t.dt.year
    weekday = t.dt.weekday
    time_data = (list(t), list(hour), list(day), list(week), list(month), list(year), list(weekday))
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")
    time_dict = {column_labels[0]:time_data[0], column_labels[1]:time_data[1], column_labels[2]:time_data[2],     column_labels[3]:time_data[3], column_labels[4]:time_data[4], column_labels[5]:time_data[5], column_labels[6]:time_data[6]}

    time_df = pd.DataFrame(time_dict)
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    #songplayId = 0
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        #songplayId = songplayId+1
        # insert songplay record
        songplay_data = (datetime.datetime.fromtimestamp(row.ts / 1e3), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


"""
    This procedure reads the files from the file system and call the processor 
    function to transform and store the information.
    
    INPUTS:
    * cur the cursor variable
    * conn the database connection
    * filepath the file location
    * func the processor function that needs to be invoked

"""
def process_data(cur, conn, filepath, func):
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
        

"""
    The main procedure that will be invoked when the script is invoked.
"""
def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
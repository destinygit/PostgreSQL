import os
import glob
import psycopg2
import json
import pandas as pd
import numpy as np
from datetime import datetime as dt, timedelta
from sql_queries import *


#Create a connection to the posgreSql Database
try:
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
except psycopg2.Error as e:
    print("Error: Could not make connection to the Postgres database")
    print(e)
    
#Get cursor to the Database
try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error: Could not get cursor to the Database")
    print(e)



def process_song_file(cur, filepath):
    """
    Description: The function processes the song file, and accepts cur and filepath arguments
    
    Arguments:
            cur: the cursor object
            filepath: song data file path
    Returns: 
            None
    """    
    # open song file
    df = pd.read_json(filepath, lines=True)
    # insert song record
    songlist = df[['song_id','title','artist_id','year','duration']]
    song_list=songlist.values[0].tolist() 
    try:
        cur.execute(song_table_insert, song_list)
    except psycopg2.Error as e:
        print("Error: Unable to insert into Song Table")
        print(e)
    conn.commit()
    
    
    # insert artist record
    df = pd.read_json(filepath, lines=True)
    df =df.rename(columns=
              {
                  "artist_id" : "artist_id",
                  "artist_name" : "name",
                  "artist_location" : "location",
                  "artist_latitude" : "latitude",
                  "artist_longitude" : "longitude"
              })

    artistlist = df[['artist_id','name', 'location', 'latitude', 'longitude']]
    artist_data = artistlist.values[0].tolist()
    try:
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.Error as e:
        print("Error: Unable to insert into Artist Table")
        print(e)
    conn.commit()
    
    


def process_log_file(cur, filepath):
    """
    Description: The function processes the log file, accepts cur and filepath arguments
    
    Arguments:
            cur: the cursor object
            filepath: log data file path
    Returns: 
            None
    """ 
    # open log file
    df = pd.read_json(filepath, lines=True)
    # filter by NextSong action
    nextsong = [df.loc[df['page'] =='NextSong']]
    nxtsong = nextsong[0]
    df = nxtsong

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = [t , t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except psycopg2.Error as e:
            print("Error: Unable to insert into Time Table")
            print(e)
        conn.commit()

    # load user table
    df =df.rename(columns=
              {
                  "userId" : "user_id",
                  "firstName" : "first_name",
                  "lastName" : "last_name",
                  "gender" : "gender",
                  "level" : "level"
              })
    df.dropna(subset = ["user_id"], inplace=True)
    userlist = df[['user_id','first_name','last_name','gender','level']]
    
    user_df = userlist

    # insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row)
        except psycopg2.Error as e:
            print("Error: Unable to insert into Time Table")
            print(e)
        conn.commit()

    # insert songplay records
    
    ts = df['ts']
    tss = np.array(ts)
    t = pd.to_datetime(ts, unit='ms')
    df['ts'].replace(tss, t, inplace =True)
    df =df.rename(columns=
              {
                  "userId" : "user_id",
                  "sessionId" : "session_id",
                  "userAgent" : "user_agent",
                  "ts" : "start_time",
                  "level" : "level"
              })
    
    # get songid and artistid from song and artist tables
    
    for index, row in df.iterrows():
        try:
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()
        except psycopg2.Error as e:
            print("Error: Unable to select song_id and artist_id")
            print(e)
        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None     
        # insert songplay record
        songplay_data = (row.start_time, row.user_id, row.level,song_id, artist_id,row.session_id, row.location, row.user_agent )
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as e:
            print("Error: Unable to insert into songplay table")
            print(e)
            conn.commit()

def process_data(cur, conn, filepath, func):
    """
    Description: This function is responsible for listing the files in a directory,
    and then executing the ingest process for each file according to the function
    that performs the transformation to save it to the database.

    Arguments:
        cur: the cursor object.
        conn: connection to the database.
        filepath: log data or song data file path.
        func: function that transforms the data and inserts it into the database.

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
    
    """
    Description: This function is responsible for processing the data of the log_file dataset
    and the song_file dataset, iterates through all the files and stores the data in the database
    tables.

    Arguments:
        cur: the cursor object.
        conn: connection to the database.
        filepath: log data or song data file path.
        func: function that transforms the data and inserts it into the database.

    Returns:
        None
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data/A', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data/2018', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()

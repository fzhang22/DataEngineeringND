import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """ 
    
    Process the raw song_data files, transform raw data into dataframe, 
    insert song record from certain columns into song table, and insert
    artist record from certain columns into artist table. Print error if
    inserting error occurs. 
    
    Parameters:
        cur (object): a cursor to perform database operations
        filepath (path): the directory name of pathname  
    """
    
    # open song file
    df = pd.read_json(filepath, lines = True)

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values.tolist()
    for row in song_data:
        try:
            cur.execute(song_table_insert, row)
        except psycopg2.Error as e:
            print("Error: Inserting Rows")
            print(e)
    
    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_longitude", 
                      "artist_longitude"]].values.tolist()
    for row in artist_data:
        try:
            cur.execute(artist_table_insert, row)
        except psycopg2.Error as e:
            print("Error: Inserting Rows")
            print(e)
            
            
def process_log_file(cur, filepath):
    """ 
    
    Process the raw log_data files, transform raw data into dataframe, 
    insert time record into time table, insert user record into user table, 
    insert songplay record into song play table. Print error if
    inserting error occurs. 
    
    Parameters:
        cur (object): a cursor to perform database operations
        filepath (path): the directory name of pathname 
    """
    
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df.loc[df["page"] == "NextSong"].reset_index()

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit = "ms")
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ["timestamp", "hour", "day", "weekOfYear", "month", "year", "weekday"]
    time_dict = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame(time_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        start_time = pd.to_datetime(row.ts, unit = "ms")
        songplay_data = (start_time, row.userId, row.level, songid, 
                         artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    
    This function get all files from directory into a list, and iteratively 
    process each file in the list by passing the function in paramater<func>
    
    Parameters:
        cur (object): a cursor to perform database operations
        filepath (path): the directory name of pathname
        func (function): the function passed as an argument
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
    
    This main function creates a new database session and a new cursor to process
    song data and log data by calling function <process_data> 
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
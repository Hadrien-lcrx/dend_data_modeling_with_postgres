import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def get_files(filepath):
    """
    Get JSON files from data folder.
    Args:
        filepath: The path (string type) to the data folder.
    Returns:
        All the files in the data folder.
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files

def process_song_file(cur, filepath):
    """
    Writes song and artist data to Postgres tables.
    Args:
        cur: A psycopg2 connection cursor.
        filepath: The path (string type) to the songs JSON data in string format.
    Returns:
        Nothing, but populates the songs and artists Postgres tables.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[["song_id", "title", "artist_id", "year", "duration"]].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Writes logs data to a pandas DataFrame.
    Args:
        cur: A psycopg2 connection cursor.
        filepath: The path (string type) to the logs JSON data.
    Returns:
        Nothing, but populates the time, user and songplay Postgres table.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit='ms')
    
    # insert time data records
    timestamp = df["ts"]
    hour = t.dt.hour
    day = t.dt.day
    week_of_year = t.dt.weekofyear
    month = t.dt.month
    year = t.dt.year
    weekday = t.dt.weekday

    time_data = (timestamp, hour, day, week_of_year, month, year, weekday)
    column_labels = ("timestamp", "hour", "day", "week_of_year", "month", "year", "weekday")
    time_dict = {}
    for k, v in zip(column_labels, time_data):
        time_dict[k] = v
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
        cur.execute(song_select, (str(row.song), str(row.artist), str(row.length)))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row["ts"], row["userId"], row["level"], songid, artistid, row["sessionId"], row["location"], row["userAgent"])
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Processes files found and indicates how many files were processed.
    Args:
        cur: A psycopg2 connection cursor.
        conn: A psycopg2 connection.
        filepath: The path (string type) to the logs JSON data.
        func: The processing function (either process_song_file() or process_log_file())
    Returns:
        Nothing, but populates the time, user and songplay Postgres table.
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
    Creates connection, gets cursor, processes files, and closes connection.
    Args:
        None.
    Returns:
        Nothing, but essentially runs the ETL pipeline.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
**1 - Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.**
Sparkify provides a streaming app and wants to analyze their users' listening behavior. To store the data, using JSON files works. But to actually analyze the data, there are better solutions. Creating an actual, organized database is a better solution for analytics. Using the right format when building the tables will ensure that the database is optimized for the business question that matters to Sparkify: understanding users' song listening patterns.

The results of this analysis about users' song listening patterns can then be used to as inputs for a future recommendation engine.


**2 - State and justify your database schema design and ETL pipeline.**
Sparkify is particularly interested in understanding what songs users are listening to. This means we need a fact table referencing song plays, and dimension tables referencing information about the features defining these songplays. Our fact table gathering songplays observations will have information about the songs themselves, as well as the artists performing them, the user who listened to them, and the moment this listening session happened. Therefore, we also need dimension tables for each of these features: songs, artists, users and times.


**3 - [Optional] Provide example queries and results for song play analysis.**
I may be using it wrong, but I wanted to run a query to get all songplays by a specific user. In `test.ipynb.` though, the columns `song_id` and `artist_id` of `songplays` are always `None`. Same with the `latitude` and `longitude` columns in `artists`.

In `etl.ipynb`, we create `songplays` using the following query:
```python
for index, row in df.iterrows():

    # get songid and artistid from song and artist tables
    cur.execute(song_select, [row.song, row.artist, row.length])
    results = cur.fetchone()
    
    if results:
        songid, artistid = results
    else:
        songid, artistid = None, None

    # insert songplay record
    songplay_data = (row["ts"], row["userId"], row["level"], songid, artistid, row["sessionId"], row["location"], row["userAgent"])
    cur.execute(songplay_table_insert, songplay_data)
    conn.commit()
```

If I `print(row.song, row.artist, row.length)`, I get a result as expected (`Ain't No Sunshine Sydney Youngblood 238.07955`), but if I `print(cur.fetchone())`, then I get `None`.  I'm following the instructions to define `song_select` in `sql_queries.py`, but if I test this query in `test.ipynb` it keeps failing because of the `'` in `Ain't No Sunshine`.

I have absolutely no clue if this is expected behavior or not. I'm nopt getting errors when running `etl.py`, but it does not mean the data written is correct. I'm alsop confused because `sql.queries` states to define songplay with 8 columns (timestamp, user ID, level, song ID, artist ID, session ID, location, and user agent and set to songplay_data) while the instructions ask for 9 columns, adding a `songplay_id` (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent), which leads to errors.
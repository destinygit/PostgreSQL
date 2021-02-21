**Sparkify Startup**

**Introduction**

A startup called **Sparkify** wants to **analyze the data** they've been collecting *on songs and user activity on their new music streaming app*. The analytics team is particularly interested in understanding **what songs users are listening to**. Currently, they don't have an easy way to query their data, which resides in a directory of *JSON logs* on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As a **data engineer** I need to create a *Postgres database* with **tables designed to optimize queries on song play analysis** for this project. My role is to create a **database schema and ETL pipeline for this analysis**. I'll be able to test the database and ETL pipeline by *running queries given to me by the analytics team* from Sparkify and *compare my results with their expected results*.

**Project Description**

In this project, I'll apply data modeling with Postgres and build an ETL pipeline using Python. To complete the project, I will *define fact and dimension tables* for a **star schema** for a particular *analytic focus*, and write an **ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.


**Objective 1:** 

>Designed a Database with five table, *Songplays_table, users_table, songs_table, artist_table and time_table*. The purpose of this Database is to store all tables records, tracking users interaction with songs in database, which song was most played and by which artist. And be able to select all the songs played by song and artist from a subset of a much larger dataset.

***Sparkify Postgres Database Design***

`Table Name: Songplays_table
column: songplay_id
column: start_time
column: user_id
column: level
column: song_id
column: artist_id
column: session_id
column: location
column: user_agent`

`Table Name: users_table
column: user_id
column: first_name
column: last_name
column: gender
column: level`

`Table Name: songs_table
column: song_id
column: title
column: artist_id
column: year
column: duration`

`Table Name: artist_table
column: artist_id
column: name
column: location
column: latitude
column: longitude`

`Table Name: time_table
column: start_time
column: hour
column: day
column: week
column: month
column: year
column: weekday`


<img src="data/Image/SparkifyStar.PNG" width="750" height="750">
             
**Implement etl.py:** 

1. On Jupyter Notebook
2. Create a Cell
3. Write this code : `%run etl.py`
4. Run cell


**Star Schema**


# DROP TABLES

songplay_table_drop = 'DROP table IF EXISTS songplay_table CASCADE'
user_table_drop = 'DROP table IF EXISTS user_table CASCADE'
song_table_drop = 'DROP table IF EXISTS song_table CASCADE'
artist_table_drop = 'DROP table IF EXISTS artist_table CASCADE'
time_table_drop = 'DROP table IF EXISTS time_table CASCADE'

# CREATE TABLES

songplay_table_create = ("CREATE TABLE IF NOT EXISTS songplay_table \
                         (songplay_id Serial Primary Key Not Null , \
                         start_time timestamp, \
                         user_id int, \
                         level varchar, \
                         song_id varchar, \
                         artist_id varchar, \
                         session_id int, \
                         location varchar, \
                         user_agent varchar);")
user_table_create = ("CREATE TABLE IF NOT EXISTS user_table \
                     (user_id int Primary Key Not Null, \
                      first_name varchar, \
                      last_name varchar,  \
                      gender varchar , \
                      level varchar);")
song_table_create = ("CREATE TABLE IF NOT EXISTS song_table \
                     (song_id varchar Primary Key Not Null, \
                     title varchar, \
                     artist_id varchar, \
                     year int, \
                     duration numeric );")
artist_table_create = ("CREATE TABLE IF NOT EXISTS artist_table \
                       (artist_id varchar Not Null, \
                        name varchar, \
                        location varchar, \
                        latitude numeric, \
                        longitude numeric);")
time_table_create = ("CREATE TABLE IF NOT EXISTS time_table \
                     (start_time timestamp Primary Key Not Null, \
                      hour numeric, \
                      day numeric, \
                      week numeric, \
                      month numeric, \
                      year numeric, \
                      weekday numeric);")

# INSERT RECORDS

songplay_table_insert = "INSERT INTO songplay_table \
                        (start_time, \
                         user_id, \
                         level, \
                         song_id, \
                         artist_id , \
                         session_id, \
                         location, \
                         user_agent) \
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO Nothing"
user_table_insert = "INSERT INTO user_table \
                        (user_id, \
                         first_name, \
                         last_name, \
                         gender, \
                         level) \
                         VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO Nothing"
song_table_insert = "INSERT INTO song_table \
                    (song_id, \
                     title, \
                     artist_id, \
                     year, \
                     duration) \
                     VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO Nothing"
artist_table_insert = "INSERT INTO artist_table \
                       (artist_id,\
                        name, \
                        location, \
                        latitude, \
                        longitude) \
                        VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO Nothing"
time_table_insert = "INSERT INTO time_table \
                     (start_time,\
                      hour, \
                      day, \
                      week, \
                      month, \
                      year, \
                      weekday) \
                      VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO Nothing"

# FIND SONGS


song_select = ("select st.song_id, at.artist_id \
                FROM song_table st \
                left join artist_table at\
                ON st.artist_id = at.artist_id")


# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]


import configparser
from datetime import datetime
import os
from os import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType

# read config files to acquire the acess to AWS services
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Return a new Sparksession that is an entry to programming Spark 
    with the Dataset and DataFrame API.
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function load song_data from S3 bucket, transform these data into 
    songs table and artists table, and write them into partitioned parquet 
    files in table directories on S3
    """
    
    # get filepath to all json file of song_data in S3 bucket
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    
    # print out the schema in tree format
    print("---------- Print out the schema of song dataset in tree format: ----------")
    df.printSchema()

    # extract columns to create songs table
    # songs table attributes: song_id, title, artist_id, year, duration
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    # show first 10 rows in songs_table
    print(" ---------- Show first 10 rows of songs table ----------")
    songs_table.show(10)
    
    # write songs table to parquet files partitioned by year and artist
    out_path_songs = os.path.join(output_data, "songs_table.parquet")
    if path.exists(out_path_songs):
        songs_table.write.parquet(path = out_path_songs, 
                                  partitionBy = ("year", "artist_id"),
                                  mode = "overwrite")
    else:
        songs_table.write.parquet(path = out_path_songs, 
                                  partitionBy = ("year", "artist_id"),
                                  mode = "append")
    
    # read parquet file and check the first 10 rows of partitioned parquet dataframes
    df_songs_parquet = spark.read.parquet("songs_table.parquet")
    print(" ---------- Show first 10 rows of songs table parquet file ----------")
    df_songs_parquet.show(10)

    # extract columns to create artists table
    # artists table attributes: artist_id, name, location, lattitude, longitude
    artists_table = df.select("artist_id", "artist_name", "artist_location", 
                              "artist_latitude", "artist_longitude")
    # show first 10 rows in artists_table
    print(" ---------- Show first 10 rows of artists table ----------")
    artists_table.show(10)
    
    # write artists table to parquet files
    out_path_artists = os.path.join(output_data, "artists_table.parquet")
    if path.exists(out_path_artists):
        artists_table.write.parquet(path = out_path_artists, 
                                    mode = "overwrite")
    else:
        artists_table.write.parquet(path = out_path_artists, 
                                    mode = "append")
    
    # read parquet file and check the first 10 rows of partitioned parquet dataframes
    df_artists_parquet = spark.read.parquet("artists_table.parquet")
    print(" ---------- Show first 10 rows of artists table parquet file ----------")
    df_artists_parquet.show(10)


def process_log_data(spark, input_data, output_data):
    """
    This function load log_data from S3 bucket, transform these data into 
    users table, time table and songplays table, and write them into 
    partitioned parquet files in table directories on S3
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # print out the schema in tree format
    print("---------- Print out the schema of log dataset in tree format: ----------")
    df.printSchema()
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table   
    # users attributes: user_id, first_name, last_name, gender, level
    users_table = df.select("userId", "firstName", "lastName", "gender", "level").distinct()
    
    # show first 10 rows in users table
    print(" ---------- Show first 10 rows of users table ----------")
    users_table.show(10)
    
    # write users table to parquet files
    output_data_users = os.path.join(output_data_users, "users_table.parquet")
    if path.exists(output_data_users):
        users_table.write.parquet(path = output_data_users, 
                                  mode = "overwrite")
    else:
        users_table.write.parquet(path = output_data_users, 
                                  mode = "append")
    
    # read parquet file and check the first 10 rows of partitioned parquet dataframes
    df_users_parquet = spark.read.parquet("users_table.parquet")
    print(" ---------- Show first 10 rows of users table parquet file ----------")
    df_users_parquet.show(10)

    # create datetime column from original timestamp column
    # divide timestamp by 1000 to convert from milliseconds to seconds
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn("start_time", get_datetime(df.ts))
    
    # time table attributes: start_time, hour, day, week, month, year, weekday
    get_hour = udf(lambda x: x.hour)  
    df = df.withColumn("hour", get_hour(df.start_time))   # create hour column
    
    get_day = udf(lambda x: x.day)
    df = df.withColumn("day",  get_day(df.start_time))  # create day column
    
    get_week = udf(lambda x: x.isocalendar()[1])
    df = df.withColumn("week", get_week(df.start_time))   # create week number column
    
    get_month = udf(lambda x: x.month)
    df = df.withColumn("month", get_month(df.start_time))  # create month column
    
    get_year = udf(lambda x: x.year)
    df = df.withColumn("year", get_year(df.start_time)) # create year column
    
    get_weekday = udf(lambda x: x.weekday())
    df = df.withColumn("weekday", get_weekday(df.start_time))  # create weekday column
    
     # extract columns to create time table
    time_table = df.select(df.columns[-7:])
    
    # show first 10 rows of time table 
    print(" ---------- Show first 10 rows of time table  ----------")
    time_table.show(10)

    # write time table to parquet files partitioned by year and month
    out_path_time = os.path.join(output_data, "time_table.parquet")
    if path.exists(out_path_time):
        time_table.write.parquet(path = out_path_time, 
                                 partitionBy = ("year", "month"),
                                 mode = "overwrite")
    else:
        time_table.write.parquet(path = out_path_time, 
                                 partitionBy = ("year", "month"),
                                 mode = "append")

    # read parquet file and check the first 10 rows of partitioned parquet dataframes
    df_time_parquet = spark.read.parquet("time_table.parquet")
    print(" ---------- Show first 10 rows of time table parquet file ----------")
    df_time_parquet.show(10)

    # read in song data to use for songplays table
    song_df = spark.read.parquet("songs_table.parquet")
    
    # inner join df with song_df by song's name
    cond = [df.song == song_df.title]
    df_join = df.join(song_df, cond, "inner")
    
    # extract columns from joined song and log datasets to create songplays table 
    # songplays attributes: songplay_id, start_time, user_id, level, song_id, 
    #                       artist_id, session_id, location, user_agent
    songplays_table = df_join.select("start_time", "userId", "level", "song_id", 
                                     "artist_id", "sessionId", "location", 
                                     "userAgent").distinct()
    
    # create songplay_id column with auto_increment
    songplays_table.withColumn("songplay_id", monotonically_increasing_id())
    
    # show first 10 rows of songplays table 
    print(" ---------- Show first 10 rows of songplays table  ----------")
    songplays_table.show(10)
    
    # append year and month column into songplays_table
    songplays_table = songplays_table.withColumn("year", get_year(df.start_time))
    songplays_table = songplays_table.withColumn("month", get_month(df.start_time))
    
    # write songplays table to parquet files partitioned by year and month
    out_path_songplays = os.path.join(output_data, "songplays_table.parquet")
    if path.exists(out_path_songplays):
        songplays_table.write.parquet(path = out_path_songplays, 
                                 partitionBy = ("year", "month"),
                                 mode = "overwrite")
    else:
        songplays_table.write.parquet(path = out_path_songplays, 
                                 partitionBy = ("year", "month"),
                                 mode = "append")
    

def main():
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://fzhang22-data-lake/parquet_files/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

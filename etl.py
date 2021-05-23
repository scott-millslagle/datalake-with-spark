import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''This function takes in a spark connection, an s3 path to the input data, and an s3
    path to the bucket where transformed data will be stored as parquet files.
    
    Keyword arguements:
    spark --  a connection to the spark app
    input_data -- the filepath to the s3 bucket where json directories are located
    output_data -- the filepath to the s3 bucket where parquet files will be stored
    
    '''
    # Read the json song data into a dataframe called song_data
    song_data = spark.read.json(input_data + "song_data/*/*/*/*.json")
    
    # Create a temp view from the song_data dataframe called songs. 
    # This table will be used to create transformed tables 
    song_data.createOrReplaceTempView("songs")
    
    # Extract columns to create songs table
    songs = spark.sql(
        '''SELECT
        DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
        FROM songs
        WHERE song_id IS NOT NULL
        AND title IS NOT NULL
        AND artist_id IS NOT NULL'''
    ) 
    
    # Write songs table to parquet files partitioned by year and artist
    songs.write.format('parquet').partitionBy('year', 'artist_id').save(output_data + 'songs')

    # Extract columns to create artists table
    artists = spark.sql(
        '''SELECT
        DISTINCT artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
        FROM songs
        WHERE artist_id IS NOT NULL
        AND artist_name IS NOT NULL'''
    )
    
    # Write artists table to parquet files
    artists.write.format('parquet').save(output_data + 'artist')


def process_log_data(spark, input_data, output_data):
    """This function takes in a spark connection, an s3 path to the input data, and an s3
    path to the bucket where transformed data will be stored as parquet files. After reading in the data
    the songs and log dataframe are joined. 
    
    Keyword arguements:
    spark --  a connection to the spark app
    input_data -- the filepath to the s3 bucket where json directories are located
    output_data -- the filepath to the s3 bucket where parquet files will be stored
    
    """
    # Read the json log data into a dataframe called log_data
    log_data = spark.read.json(input_data + "log-data/*/*/*.json")

    # Filter log_data on NextSong and create a column called time_stamp
    log_data = log_data.filter(log_data.page == "NextSong").withColumn(
        'start_time', from_unixtime(log_data.ts/1000)
    ).drop('ts')
    
    # Create a temp view from the log_data dataframe called logs. 
    # This table will be used to create transformed tables 
    log_data.createOrReplaceTempView("logs")
    
    # Read the json song data into a dataframe called song_data
    song_data = spark.read.json(input_data + "song_data/*/*/*/*.json")
    
    # Create a temp view from the song_data dataframe called songs. 
    # This table will be used to join to logs table
    song_data.createOrReplaceTempView("songs")
    
    # Join songs and logs tables 
    joined_data = spark.sql(
        '''SELECT *
        FROM logs l
        LEFT OUTER JOIN songs s
        ON (l.artist = s.artist_name
        AND l.song = s.title
        AND l.length = s.duration)'''
    )
    
    # Create a temp view from logs and songs tables called raw_data
    joined_data.createOrReplaceTempView('raw_data')

    # extract columns for users table    
    users = spark.sql(
        '''SELECT
        DISTINCT userId,
        firstName,
        lastName,
        gender,
        level
        FROM raw_data rd1
        WHERE start_time = (SELECT MAX(start_time) FROM raw_data rd2
        WHERE rd1.userId = rd2.userId)'''
    ) 
    
    # Write users table to parquet files
    users.write.format('parquet').save(output_data + 'users')

    
    # Extract columns to create time table
    time = spark.sql(
        '''SELECT DISTINCT start_time,
        date_format(start_time, 'HH') AS hour,
        date_format(start_time, 'dd') AS day,
        date_format(start_time, 'F') AS week,
        date_format(start_time, 'MM') AS month,
        date_format(start_time, 'y') AS year,
        date_format(start_time, 'E') AS weekday
        FROM raw_data'''
    )
    
    # Write time table to parquet files partitioned by year and month
    time.write.format('parquet').partitionBy('year', 'month').save(output_data  + 'time')

    # Create a temp view of the time table for easy songplay fact table creation
    time.createOrReplaceTempView('time_table')
    
    # Extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql(
        '''SELECT
        monotonically_increasing_id() AS songplay_id,
        t.start_time,
        userId,
        level,
        song_id,
        artist_id,
        sessionId,
        location,
        userAgent,
        t.year,
        t.month
        FROM raw_data r
        LEFT JOIN time_table t
        ON r.start_time = t.start_time'''
    )

    # write songplays table to parquet files partitioned by year and month
    songplay_table.write.format('parquet').partitionBy('year', 'month').save(output_data + 'songplays')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "Input your bucket path here"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

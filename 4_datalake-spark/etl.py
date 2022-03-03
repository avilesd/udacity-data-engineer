import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear, to_timestamp

config = configparser.ConfigParser()
config.read('dl.cfg')

# Note: We added the [AWS] header in the .cfg file
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
   - Reads the JSON data from the filepath (`song_data`) as a dataframe, it then creates two tables and saves them in parquet format.

   - Creates tables `songs` and `artists` by selecting the proper columns.
   """

    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df \
        .select(col('song_id'), col('title'), col('artist_id'), col('year'), col('duration')) \
        .distinct()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(
        os.path.join(output_data, 'analytics/songs/'))

    # extract columns to create artists table
    artists_table = df \
        .select(col('artist_id'), col('artist_name'), col('artist_location'),
                col('artist_latitude'), col('artist_longitude')) \
        .distinct()

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data, 'analytics/artists/'))


def process_log_data(spark, input_data, output_data):
    """
    - Reads the JSON data from the filepath (`log_data`) as a dataframe, it then creates three tables and saves them in parquet format.

    - Creates tables `time`, `users` and `songplays` by selecting and transforming the proper columns.
    """

    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/')

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where(col('page') == 'NextSong')

    # extract columns for users table
    users_table = df \
        .select(col('userId'), col('firstName'), col('lastName'), col('gender'), col('level')) \
        .distinct()

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path.join(output_data, 'analytics/users/'))

    # create timestamp column from original timestamp column
    #     get_timestamp = udf()
    #     df =

    # create datetime column from original timestamp column
    #     get_datetime = udf()
    #     df =

    # extract columns to create time table
    time_table = df.withColumn('ts_timestamp', to_timestamp(col('ts') / 1000)) \
        .select(col('ts').alias('start_time'),
                hour(col('ts_timestamp')).alias('hour'),
                dayofmonth(col('ts_timestamp')).alias('day'),
                weekofyear(col('ts_timestamp')).alias('week'),
                month(col('ts_timestamp')).alias('month'),
                year(col('ts_timestamp')).alias('year'),
                dayofweek(col('ts_timestamp')).alias('weekday')) \
        .distinct()

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(
        os.path.join(output_data, 'analytics/time/'))

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, 'song_data/*/*/*/*.json'))

    # extract columns from joined song and log datasets to create songplays table
    df = df.alias('df')
    song_df = song_df.alias('df_song')

    songplays_table = df \
        .join(song_df, on=(col('df.artist') == col('df_song.artist_name')) \
                          & (col('df.song') == col('df_song.title')) & (col('df.length') == col('df_song.duration')),
              how='left') \
        .where((col('userId').isNotNull()) & (col('ts').isNotNull())) \

    songplays_table = songplays_table \
        .join(time_table, on=(songplays_table.ts == time_table.start_time), how='left') \
        .drop(col('df_song.year')) \
        .select(monotonically_increasing_id().alias('songplays_id'), col('start_time'),
                col('userId'), col('level'), col('song_id'), col('artist_id'), col('sessionId'),
                col('location'), col('userAgent'), col('year'), col('month')) \
        .distinct()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(
        os.path.join(output_data, 'analytics/songplays/'))


def main():
    """
    - Creates a spark session and uses it to run the `process_song_data` and `process_log_data` functions.

    - Using the variables `input_data` and `output_data` you can control where your data is coming from and going to, respectively.
    """
    spark = create_spark_session()
    input_data = "data/"
    output_data = "output/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

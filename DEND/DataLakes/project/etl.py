import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_timestamp
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df['song_id','title','artist_id','year','duration']
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write \
                .partitionBy(['year','artist_id']) \
                .format('parquet') \
                .mode('overwrite') \
                .save(os.path.join(output_data,"songs"))

    # extract columns to create artists table
    artists_table = df.selectExpr('artist_id' \
                           ,'artist_name as name' \
                           ,'artist_location as location' \
                           ,'artist_latitude as latitude' \
                           ,'artist_longitude as longitude' \
                           ,'year')
    artists_table = artists_table.withColumn('rownum', F.row_number() \
                                .over(Window.partitionBy('artist_id') \
                                              .orderBy(F.desc('year'))))
    artists_table = artists_table.filter(users_df.rownum == 1)
    artists_table.drop('rownum')
    artists_table.drop('year')
    
    # write artists table to parquet files
    artists_table.write \
                .format('parquet') \
                .mode('overwrite') \
                .save(os.path.join(output_data,'artists'))


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_cleaned = df.filter(df.page == 'NextSong')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: to_timestamp(x / 1000.0))
    df_cleaned = df_cleaned.withColumn('ts_timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: date_format(x, 'yyyy.MM.dd HH:mm:ss.SSS'))
    df_cleaned = df_cleaned.withColumn('datetime', get_datetime(df.ts_timestamp))

    # extract columns for users table    
    users_df = df_cleaned.selectExpr('userId as user_id' \
                           ,'firstName as first_name' \
                           ,'lastName as last_name' \
                           ,'gender as gender' \
                           ,'level as level' \
                           ,'ts_timestamp as date')
    users_df = users_df.withColumn('rownum', F.row_number() \
                                .over(Window.partitionBy('user_id') \
                                              .orderBy(F.desc('ts_timestamp'))))
    users_table = users_df.filter(users_df.rownum == 1)
    users_table.drop('rownum')
    
    # write users table to parquet files
    users_table.write \
                .format('parquet') \
                .mode('overwrite') \
                .save(os.path.join(output_data,'users'))
    
    # extract columns to create time table
    time_table = df_cleaned.selectExpr('ts_timestamp as start_time',
                                       'year(ts_timestamp) as year',
                                       'hour(ts_timestamp) as hour',
                                       'month(ts_timestamp) as month',
                                       'day(ts_timestamp) as day',
                                       'weekofyear(ts_timestamp) as week',
                                       'dayofweek(ts_timestamp) as weekday')
    
    # write time table to parquet files partitioned by year and month
    time_table.write \
                .partitionBy(['year','month']) \
                .format('parquet') \
                .mode('overwrite') \
                .save(os.path.join(output_data,"time"))

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, 'song_data/*/*/*/*.json'))

    # extract columns from joined song and log datasets to create songplays table 
    song_log_join_df = df_cleaned.join(song_df, (df_cleaned.song == song_df.title) & (df_cleaned.artist == song_df.artist_name),'left_outer')
    song_log_join_df = song_log_join_df.withColumn("songplay_id", monotonically_increasing_id())
    songplays_table = song_log_join_df.selectExpr('songplay_id',
                                               'ts_timestamp as starttime',
                                               'userId as user_id',
                                               'level',
                                               'song_id',
                                               'artist_id',
                                               'sessionId as session_id',
                                               'location',
                                               'userAgent as user_agent'
                                              )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write \
            .format('parquet') \
            .mode('overwrite') \
            .save(os.path.join(output_data,"songplays"))

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-datalakes/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

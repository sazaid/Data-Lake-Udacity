import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import databricks.koalas as ks
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format




config = configparser.ConfigParser()

# Normally this file should be in ~/.aws/credentials
config.read('dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']

# Create spark session with hadoop-aws package
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    # get filepath to song data file
    song_data = "data/song_data/*/*/*/*.json"
    
    # read song data file
    kdf = ks.read_json(song_data)
    
    
    # extract columns to create songs table dimension
    songs_table = (ks.sql('''
               SELECT 
               DISTINCT
               song_id,
               title,
               artist_id,
               year,
               duration
               FROM 
                   {kdf}''')
              )

    
    # write songs table parquet files partitioned by year and artist
    songs_table.to_spark().write.mode('overwrite').partitionBy("year", "artist_id").parquet('songs/')
    
    # extract columns to create artists table dimension
    artists_table = (ks.sql('''
               SELECT 
               DISTINCT
               artist_id,
               artist_name,
               artist_location,
               artist_longitude,
               artist_latitude
               FROM 
                   {kdf}''')
              )
    
    # write artists table to parquet files 
    artists_table.to_spark().write.mode('overwrite').parquet('artists/')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = "data/*.json"

    # read log data file
    df_log = ks.read_json(log_data)  
    
    

    # extract columns for users table dimension    
    users_table = (ks.sql('''
               SELECT 
               DISTINCT
               userId,
               firstName,
               lastName,
               gender,
               level
               FROM 
                   {df_log}''')
              )
    
    # write users table to parquet files
    users_table.to_spark().write.mode('overwrite').parquet('users/')

    
    # get timestamp and datetime
    unix_time_series = df_log.head().ts.copy()
    get_timestamp_datetime = ks.DataFrame(data = unix_time_series)                   
 

    # extract columns to create time table
    get_timestamp_datetime.pipe(extract_time_features)
    
    # write time table to parquet files partitioned by year and month
    get_timestamp_datetime.to_spark().write.mode('overwrite').partitionBy("year", "month").parquet('time/')

    
    
    # read in song data to use for songplays table
    df_song = ks.read_json("data/song_data/*/*/*/*.json")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = (ks.sql('''SELECT e.ts,
       t.month,
       t.year,
       e.userId,
       e.level,
       s.song_id,
       s.artist_id,
       e.sessionId,
       e.location,
       e.userAgent
       FROM 
       {df_log} e
       JOIN {df_song} s ON (e.song = s.title AND e.artist = s.artist_name) 
       JOIN {get_timestamp_datetime} t ON (e.ts = t.ts)''')   
                  )
   
    # write songplays table to parquet files partitioned by year and month
    songplays_table.to_spark().write.mode('overwrite').partitionBy("year", "month").parquet('songplay/')
    
    
# create timestamp column from original timestamp column
def extract_time_features(kdf):

    kdf['timestamp'] = ks.to_datetime(kdf.ts, unit='ms')
    kdf['hour'] = kdf.timestamp.dt.hour
    kdf['dayofweek'] = kdf.timestamp.dt.dayofweek
    kdf['year'] = kdf.timestamp.dt.year
    kdf['month'] = kdf.timestamp.dt.month              
    return kdf

def main():
    spark = create_spark_session()
   # Load data from S3
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://project4dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

# Data-Lake-Udacity

**purpose of this database:**

# Sparkify is a music streaming startup with a growing user base and song database. Their user activity and songs metadata data resides in json files in S3. The goal of the project is to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

 

**Database schema design and ETL pipeline*

# Step 1 : Create spark session with hadoop-aws package.
# Step 2 : Get file path to song data file and read song data file.
# Extract columns to create songs table and write songs table parquet files partitioned by year and artist
# Extract columns to create artists table and write artists table to parquet files

# Step 3 : Get file path to log data file and read log data file.
# Extract columns for users table and write users table to parquet files.
# Create timestamp column from original timestamp column and write time table to parquet files partitioned by year and month.

# Step 4 : Read in song data to use for songplays table and extract columns from joined song and log datasets to create songplays table and  write songplays table to parquet files partitioned by year and month. 

# Finally I Run the etl.py to reads data from S3, processes that data and writes them back to S3.

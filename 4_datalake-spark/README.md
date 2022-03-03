# Introduction
Goal and reason for this document: This file is the documentation for the project 'Data Lake' for a ficticious startup called 'Sparkify'.

# Project Description
What: We are part of the data engineering teem from a startup called 'Sparkify'. One of their products is a music streaming app. The startup has collected data on songs (see `song_data`) and user activity within the app (see `log_data`).

Why: The collected data currently exists as JSON metadata (`song_data`)  and as JSON logs (`log_data`) in S3 (AWS). In order for the analytics team to make use of this data and gain insights from it, we as data engineers have to prepare it first, i.e. through an **ETL-pipeline**.

# Approach

We are going to use to use a 'Data Lake' approach to 1. read the raw data, 2. process and transform it and 3. writing to data storage. From there, the analytics team can easily access the analytics tables.

Technically, we are going to use PySpark, in this case a pyspark shell script that leverages the Spark technology to do all three steps described above.

## Files
### etl.py
This is the main document, which contains the ETL pipeline:

This file contains two modular functions which both read data in raw-format (here .json), filter/select/transform/partition it if necessary and save it as parquet-format.

1. The `process_song_data` function is built to read multiple `song_data` JSON-files at once and create one single dataframe. From that dataframe, both the `songs` and the `artits` tables by selectively chosing the desired columns. To illustrate this:
    - The `songs`table contains the columns: ("song_id", "title", "artist_id", "year", "duration")
    - The `artists` table contains the columns ("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    
2. The `process_log_data` also reads multiple JSON-files, aka `log_data` and joins them into one single data frame. The process is similar to the former function, with the addition of extra steps for the `time` table and the `songplays` table:
    - For the `time` table we first convert the numeric timestamp in miliseconds to a proper timestamp and split it into useful, modular time information (hour, day, month, etc.)
    - For the `songplays`table we use information from both the `log_data` and `song_data` to match for `artist_id` and `song_id`. For this, we have to read the `song_data` again and join it with the `log_data`.

Since the idea is to read and write directly from S3, there is no need to create a connection of some sort (e.g. to a database).

### Output-Validation.ipynb
This document is not part of the functionality of this program as a whole. It is there only to validate and check if the output tables have the desired Schema and Structure. A data quality-check of sorts.

# How to run the Python script
First of all, make sure to open the `etl.py` file, scroll to the main() function and if necessary, edit the `input_data` and `output_data` variables. Also, remember to put your AWS information in a `dl.cfg` file.

Then, assuming you have the dependencies for PySpark, you can open the command shell and type `python etl.py`. If the scripts runs successfully, you will find five new folders in your `output_data` location containing the parquet partitions for the five tables.

# References
- Understanding how S3 handles zip files: https://knowledge.udacity.com/questions/344088
- On partitioning by columns: https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrameWriter.partitionBy.html?highlight=partition#pyspark.sql.DataFrameWriter.partitionBy
- Refresh how to join on multiple columns in Pyspark: https://www.geeksforgeeks.org/how-to-join-on-multiple-columns-in-pyspark
- On reading nested .json files, e.g. for the song_data: https://knowledge.udacity.com/questions/127025
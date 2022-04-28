#!/usr/bin/env python
# coding: utf-8

# # Data Preparation for the Analysis of Tourism Visits in the United States from Mexico and the Northern Triangle (El Salvador, Guatemala, Honduras)
# ### Data Engineering Capstone Project
# This project and this document are part of the final project for the Data Engineering Nanodegree from Udacity (a.k.a. "The Capstone Project"). The goal is to use the skills and methods learned within the program, to run a Data Engineering project from scratch. We will use the project provided by Udacity, in which a large dataset of arrivals into the United States from all over the world, forms the basis of the data.
# 
# #### Project Summary
# In this project, we start with a clear **data-driven goal**, from which we derive a **series of steps** to reach that goal using **large quantities of data**. This intends to replicate the typical process a data engineer goes through on a regular basis.

# Do all module imports and installs here
import pandas as pd
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DecimalType

# Create and configure Spark Session
spark = SparkSession.builder.config("spark.jars.repositories", "https://repos.spark-packages.org/").config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").enableHiveSupport().getOrCreate()

# UDF Functions
from datetime import datetime, timedelta
from pyspark.sql import types as T
# Reference: https://knowledge.udacity.com/questions/66798
def to_datetime_sas(x):
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(x))
    except:
        return None
udf_to_datetime_sas = udf(lambda x: to_datetime_sas(x), T.DateType())


# ### Step 1: Scope the Project and Gather Data
# Read immigration data
dir_path = '../../data/18-83510-I94-Data-2016/'
iteration = 0
relevant_country_codes = [582.0, 528.0, 576.0, 577.0]

for file in os.listdir(dir_path):
    if iteration == 0:
        df_immigration_month = spark.read.format('com.github.saurfang.sas.spark')        .load(os.path.join(dir_path, file))        .filter(col('i94cit').isin(relevant_country_codes))
    else:
        df_temp = spark.read.format('com.github.saurfang.sas.spark')        .load(os.path.join(dir_path, file))        .filter(col('i94cit').isin(relevant_country_codes))
        
        if df_immigration_month.columns != df_temp.columns:
            print(f"The columns in {file} are different then the ones until one. Deleting unnecesary columns...")
            df_temp = df_temp.drop('validres', 'delete_days', 'delete_mexl', 'delete_dup', 'delete_visa', 'delete_recdup')
            
            if df_immigration_month.columns != df_temp.columns:
                print("Columns are still not matching")
            else:
                print("Columns now match. Appending...")
        else:    
            df_immigration_month = df_immigration_month.union(df_temp)

    iteration += 1

# Read Temperature data
df_temperature = spark.read.format('csv').option('header', 'true').load('../../data2/GlobalLandTemperaturesByCity.csv')


# Read convertion table for the port codes
df_port_code_names = spark.read.format('csv').option('header', 'true').option('sep', ';').load('port_codes.csv')


# ### Step 2: Explore and Assess the Data
# #### Explore the Data 

# Drop three columns
df_immigration_month = df_immigration_month.drop('count', 'dtadfile', 'i94bir')

# Convert relevant numeric type columns to IntegerType()
int_type_cols = ['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94addr', 'i94res', 'i94mode', 'i94visa', 'biryear']
df_imm_clean_1 = df_immigration_month

for i in int_type_cols:
    df_imm_clean_1 = df_imm_clean_1.withColumn(i, df_imm_clean_1[i].cast(IntegerType()))


# Convert all three date columns `arrdate`, `depdate` ,`dtaddto` to a proper string date format: 'yyyy-MM-DD'

# arrdate and depdate to dateformat and yyyy-MM-dd
df_imm_clean_2 = df_imm_clean_1.withColumn('arrdate', udf_to_datetime_sas('arrdate')).withColumn('depdate', udf_to_datetime_sas('depdate'))

# dtaddto restring to yyyy-MM-dd
df_imm_clean_2 = df_imm_clean_2.withColumn('dtaddto', to_date(col('dtaddto'), 'MMddyyyy'))

df_imm_clean_3 = df_imm_clean_2.drop('i94addr', 'visapost', 'occup', 'insnum', 'entdepu', 'entdepa', 'entdepd', 'matflag')

df_imm_clean_4 = df_imm_clean_3.filter(col('i94visa') == 2).drop('i94visa', 'i94yr', 'i94mon')


# #### 2.2 Explore and clean Temperature Data

# Filter for temperature data in the United States & drop the `Country` column
df_temp_0 = df_temperature.filter(col('Country') == 'United States').drop('Country')


# Convert date column `dt` from string to date, for easier filtering and correct data representation type
df_temp_1 = df_temp_0.withColumn('dt', to_date(col('dt'), 'yyyy-MM-dd')).filter(col('dt') >= datetime(2008, 9, 1))

# Convert temperature data to numeric
df_temp_1 = df_temp_1.withColumn('AverageTemperature', col('AverageTemperature').cast(DecimalType())).withColumn('AverageTemperatureUncertainty', col('AverageTemperatureUncertainty').cast(DecimalType()))


df_temp_2 = df_temp_1.filter(~isnull(col('AverageTemperature')))

# Remove duplicate data
df_temp_3 = df_temp_2.distinct()

df_temp_4 = df_temp_3.groupBy(col('dt'), col('City')).agg(
    mean('AverageTemperature').alias('avg_temperature'),
    mean('AverageTemperatureUncertainty').alias('avg_temperature_uncertainty'),
    first('Latitude').alias('latitude'),
    first('Longitude').alias('longitude')
)

# ### Step 4: Run Pipelines to Model the Data & Data Quality Checks
# #### 4.1 Create the data model
# Build the data pipelines to create the data model.

# 3.2.1 Creating 'port_temperature'
# Create the month_of_year and day_of_month column
df_temp_5 = df_temp_4.withColumn('month_of_year', date_format(col("dt"), "MM"))
# Group by the combination of day_of_month and month_of_year
df_temp_6 = df_temp_5.groupBy(col('month_of_year'), col('City')).agg(
    mean('avg_temperature').alias('avg_temperature'),
    mean('avg_temperature_uncertainty').alias('avg_temperature_uncertainty'),
    first('latitude').alias('latitude'),
    first('longitude').alias('longitude')
)

# Add a unique id as a primary key for the table 'port_temperature'
df_temp_final = df_temp_6.select(monotonically_increasing_id().alias('city_temp_id'),
        col('month_of_year'),
        col('City').alias('city'),
        col('avg_temperature'),
        col('avg_temperature_uncertainty'),
        col('latitude'),
        col('longitude'))


df_temp_final.write.mode('overwrite').partitionBy('month_of_year').parquet('output/port_temperature/')

# 3.2.2 Creating 'tourist_entries'
df_imm_prep_1 = df_imm_clean_4.withColumn('arrival_month', date_format(col('arrdate'), 'dd'))

df_imm_prep_2 = df_imm_prep_1.join(df_port_code_names, on=(df_imm_prep_1.i94port == df_port_code_names.port_code) , how = 'left')

tourist_entries = df_imm_prep_2.alias('imm').join(df_temp_final.alias('temp'), expr("imm.port_name == upper(temp.city) AND imm.arrival_month == temp.month_of_year"), how='left')

tourist_entries = tourist_entries.select(col('cicid').alias('cicid'),
    col('arrdate').alias('arrival_date'),
    col('depdate').alias('departure_date'),
    col('i94port').alias('port_code'),
    col('port_name'),
    col('city_temp_id'),
    col('i94mode').alias('entry_mode'),
    col('dtaddto').alias('date_added'),
    col('airline'),
    col('fltno').alias('flight_number'),
    col('admnum').alias('admission_id'))


tourist_entries.write.mode('overwrite').parquet('output/tourist_entries')

# 3.2.3 Creating 'travelers'
travelers = df_imm_clean_4 .select(col('admnum').alias('admission_id'),
    col('i94cit').alias('citizen_of'),
    col('i94res').alias('resident_of'),
    col('biryear').alias('year_of_birth'),
    col('gender'),
    col('visatype').alias('visa_type')) \
.distinct()


travelers.write.mode('overwrite').partitionBy('year_of_birth').parquet('output/travelers/')


# #### 4.2 Data Quality Checks
# Explain the data quality checks you'll perform to ensure the pipeline ran as expected.

# - We will perform three data quality checks, two integrity constraints on the unique keys and one count check over all tables:
#     1. Quality check 1 (QC1): will consist of checking for missing values in the primary key of each table. For this we, select just the column containig the primary key, filter for existing null-values and then take a count. If the value is any other than zero, we print a warning. Since PySpark nor Parquet (or chosen output data-format) do not enforce this type of integrity constraints - as opposed to a Postgres database - it makes sense to check them after running the data pipelines. The same applies for the next check.
#     2. Quality check 2 (QC2): will check if the primary key column of each of the three tables contains unique values. To do this, we will compare the count of the full table against the distinct values of the primary key column. If they do not match, we print a warning.
#     3. Quality check 3 (QC3): in this count check, we make sure that non of the tables are empty after the data pipelines have ran. If this is the case, we also print a warning.

# Quality check 1: Checking for missing values in the primary key
temp_id_missing_value_count = df_temp_final.select('city_temp_id').filter(isnull(col('city_temp_id'))).count()

if temp_id_missing_value_count != 0:
    print('Warning: The primary key *city_temp_id* in the port_temperature table contains missing values')
else:
    print('Success: The data check ran successfully > The primary key in the port_temperature table does not contain missing values')

# Quality check 1: Checking for missing values in the primary key
entries_id_missing_value_count = tourist_entries.select(col('cicid')).filter(isnull(col('cicid'))).count()

if entries_id_missing_value_count != 0:
    print('Warning: The primary key *cicid* in the tourist_entries table contains missing values')
else:
    print('Success: The data check ran successfully > The primary key in the tourist_entries table does not contain missing values')


# Quality check 1: Checking for missing values in the primary key
travelers_id_missing_value_count = travelers.select(col('admission_id')).filter(isnull(col('admission_id'))).count()

if travelers_id_missing_value_count != 0:
    print('Warning: The primary key *admission_id* in the travelers table contains missing values')
else:
    print('Success: The data check ran successfully > The primary key in the travelers table does not contain missing values')


# ##### QC2: Run check for the unique-constraint on the primary keys of all three tables

# Quality check 2: Check for unique values in the primary key
row_count_full = df_temp_final.count()
row_count_primary_key = df_temp_final.select(col('city_temp_id')).count()

if row_count_full != row_count_primary_key:
    print('Warning: The primary key in the port_temperature table contains duplicate values')
else:
    print('Success: The primary key in the port_temperature table contains only unique values')

# Quality check 2: Check for unique values in the primary key
row_count_full = tourist_entries.count()
row_count_primary_key = tourist_entries.select(col('cicid')).count()

if row_count_full != row_count_primary_key:
    print('Warning: The primary key in the tourist_entries table contains duplicate values')
else:
    print('Success: The primary key in the tourist_entries table contains only unique values')


# Quality check 2: Check for unique values in the primary key
row_count_full = travelers.count()
row_count_primary_key = travelers.select(col('admission_id')).count()

if row_count_full != row_count_primary_key:
    print('Warning: The primary key in the travelers table contains duplicate values')
else:
    print('Success: The primary key in the travelers table contains only unique values')


# ##### QC3: Run check for non empty tables
# - Check that the tables indeed are integral and have some rows and are not fully empty

# Quality check 3: check for existing rows in the table
table_row_count = df_temp_final.count()

if table_row_count == 0:
    print('Warning: The port_temperature table appears to be empty')
else:
    print('Success: The port_temperature table is not empty')

# Quality check 3: check for existing rows in the table
table_row_count = tourist_entries.count()

if table_row_count == 0:
    print('Warning: The tourist_entries table appears to be empty')
else:
    print('Success: The tourist_entries table is not empty')

# Quality check 3: check for existing rows in the table
table_row_count = travelers.count()

if table_row_count == 0:
    print('Warning: The travelers table appears to be empty')
else:
    print('Success: The travelers table is not empty')

print('Stopping Spark Session')
spark.stop()


# Introduction
Goal and reason for this document: This file is the documentation for the project 'Data Warehouse' for a ficticious startup called 'Sparkify'.

# Project Description
What: We are part of the data engineering teem from a startup called 'Sparkify'. One of their products is a music streaming app. The startup has collected data on songs (see `song_data`) and user activity within the app (see `log_data`).

Why: The collected data currently exists as JSON metadata (`song_data`)  and as JSON logs (`log_data`) in S3 (AWS). In order for the analytics team to make use of this data and gain insights from it, we as data engineers have to prepare it first, i.e. through an **ELT-pipeline** (not etl).

How 1 - conceptually, the process works in two steps:
1. First **load** the data from storage S3 into staging tables in Redshift. Loading the data first in Redshift will allow us to transform/filter the data into the shape and form we need, which is:
2. Use the staging tables to model the data into a star schema (here also called analytical tables), as we saw in the project: "Data Modeling with Progres". For details on the data modeling see (https://github.com/avilesd/udacity-data-engineer/tree/main/1-data-modeling-postgres)

How to read this document: this documentation is document oriented. So each chapter below describes different contents of the assignment in relation to the workspace files:

How 2 - programatically, we have created three process files and one config files:
1. `sql_queries.py` : Python document where all the SQL queries will be written and saved. There are four clusters of queries:
    1. Drop-queries: drop tables if they exist in a given database
    2. Create-queries: create tables from scratch in a given database. The queries create both the staging and the analytical tables.
    3. Copy-queries: copy the data from S3 into the staging-tables.
    4. Insert-queries: insert the data from the staging-tables into the final analytical tables
2. `create_tables.py`: Python document where the tables are 'refreshed' in the sense that they are dropped if they exist and then created again from scratch. This is done by using the drop-queries (1.) and the create-queries (2.), respectively.
3. `etl.py`: This file contains the ELT pipeline (even though is called e**tl**.py), where data is first loaded from S3 into staging tables and then transformed into the analytical tables (one fact table and four dimensional tables). For this task, this file uses the copy-queries (3.) and insert-queries (4.), respectively.
4. `dwh.cfg`: It contains configuration information for the cluster on AWS and paths for the data in S3. Note: this file will not be included when turning in the project, since it contains sensitive information. 

Next you find a short instruction on how to run these scripts and make them work together.

# How to run the Python scripts
- Conditions / Assumptions: a redshift cluster has been created and an IAM-role attached to it, which can access the data in S3
- enter the necessary config-information in `dwh.cfg` to connect to your redshift cluster and the necessary authentication information to connect to it. Furthermore enter the relevant S3 paths and config-info.
- open the terminal and navigate to your workspace
- within the terminal
    - run `python create_tables.py`
    - run `python sql_queries.py`
    - run `python etl.py`
    - (optional) to validate the output, log-in to the Redshift web-interface and run some analytical queries on your tables
    - if the cluster and data is no longer needed, do not forget to clean your resources (e.g. by deleting the cluster)

## On Tables and Queries
Regarding the 'songplays' table: since the exercise did not specify, that we only had to filter for 'NextSong' events, we didn't filter for these rows when loading the analytical tables.

## On choosing the distribution style (distkey or diststyle all)
Since the 'songplays' table is expected to be the largest, we will use the distkey keyword for distribution instead of e.g. a 'distribution all' strategy. Since the 'songs' and 'artists' ids share the ``artist_id`` with the 'songplays' table, we will use this attribute as the 'distkey' on all three tables.

The rest of the dimension tables ('users', 'time') we expect to be smaller in relation to the 'songplays', but still quite large for an 'diststyle all'. We will try the 'diststyle auto' strategy for these tables, to let redshift decide the best strategy. Though this could be a possible point of improvement/change later on.

## On choosing a column as sortkey
We will pick a sortkey for every fact and dimension table, but not for the staging tables. The logic we are going to follow for picking a column as the sorting key is:
1. preferably an integer column and
2. whichever column within that table the analytic-queries are likely to do "order by" on

## Resources
On the IDENTITY(seed, step) instead of SERIAL for Redshift.
"When you add rows using an INSERT or INSERT INTO [tablename] VALUES() statement, these values start with the value specified as seed and increment by the number specified as step."

## Sources
How to use the config parser: https://docs.python.org/3/library/configparser.html
How to load data from S3 using the COPY command: https://www.bmc.com/blogs/amazon-redshift-load-data/
Helpful comments: https://knowledge.udacity.com/questions/62116
On giving me an idea how to do populate the analytical tables in an efficient way, instead of using a for loop: https://knowledge.udacity.com/questions/624650, https://knowledge.udacity.com/questions/673723
Help on the issue of modelling ts in staging tables as bigint and then having to convert it to timestamp in Redshift: https://knowledge.udacity.com/questions/154533, https://docs.aws.amazon.com/redshift/latest/dg/r_EXTRACT_function.html
On understanding the practical purpose of staging tables: https://knowledge.udacity.com/questions/317683
On understanding joining on redshift: https://docs.aws.amazon.com/redshift/latest/dg/r_Join_examples.html

## Possible improvement ideas
[] Add references-constrains in CREATE statements, e.g.: `...sp_song_id varchar REFERENCES songs (so_song_id)...`
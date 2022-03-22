# Introduction
Goal and reason for this document: This file is the documentation for the project 'Data Modeling with Progres' for a ficticious startup called 'Sparkify'.

# Project Description
What: We are part of the data engineering teem from a startup called 'Sparkify'. One of their products is a music streaming app. The startup has collected data on songs (see `song_data`) and user activity within the app (see `log_data`). The analytics teams wants to use this data to gain insight into the songs users are listening to.

Why and how: The collected data currently exists as JSON metadata (`song_data`)  and as JSON logs (`log_data`). The analytics team has asked for our support to deliver a database, where they can easily run analytical queries. In order to fulfill this requirement, we will procede as follows:

1. Data Modelling: Define and create a database schema and its tables. This will be done with PostgreSQL.
2. ETL pipelining: Design an extract, transform and load pipeline to systematically populate the database using the `song_data` and `log_data`
3. Testing: Use the queries from the analytics team to test validate the results of the first two steps

Overview of the ETL-process and data flow:
![image](https://user-images.githubusercontent.com/9051749/159493768-823cb578-e366-4650-be42-0fabe540ed90.png)


How to read this document: this documentation is document oriented. So each chapter below describes different contents of the assignment in relation to the workspace files:

1. `sql_queries.py` : Python document where all the SQL queries will be written and saved
2. `create_tables.py`: Python document where the tables are 'refreshed' in the sense that they are dropped (if they exist) and then created from scratch
3. `etl.ipynb`: Extract, Transform, Load (ETL) Process
4. `etl.py`: ETL pipeline
5. `test.ipynb`: The main purpose of this document is to do some SELECT statements on all the created tables, to check if the queries are working as expected. To be used in development and for testing 'final' output for this assignment

At the end of the document you find a short instruction on how to run these scripts and make them work together.

### Technology used and versions:
- Python, version: 3.x
- PostgreSQL, version: unknown
- Python packages in use --> see both `*.ipynb` files

# sql_queries.py
This document contains the necessary SQL queries for the project written in python.

## Schema and tables
The schema of this project will be a **star schema**. It consists of one fact table called **songplays** and four dimension tables: **users**, **songs**, **artists** and **time**.

The following are some basic reasons, why we chose a star schema for this project:
- since our 'clients' - the analytics team - is mostly interested in 'understanding what songs users are listening to', it feels natural that  we organize our data around a table with this exact information --> result: **songplays** as fact table
- the  analytics nature of the project:
    - requires the data structure to be able to deliver simple joins and simple queries in an efficient way
    - requires fast aggregations, which again are well served with a star schema
- simplicity of star schema over e.g. the snowflake schema

The queries have been clustered (from the assignment) in four groups: DROP, CREATE, INSERT and SELECT statements. At the end the reader can also find a python list of the CREATE and DROP queries for easy, iterative access to the queries (see file `create_tables.py`).

#### Notes on CREATE and DROP Queries
- For the DROP Queries a "IF EXISTS" clause was added to avoid an error from`create_tables.py`

# create_tables.py
Python document where the tables are 'refreshed' in the sense that they are dropped (if they exist) and then created from scratch

### Adding try and except clauses
Since we are dealing with the creation, connection and cursor-executer of a database, we inserted some `try`and `except` clauses for `psycopg2` errors. The placement of the clauses was not exhaustive but pragmatic, i.e. they were placed where needed most.

# etl.ipynb
The process of of Extracting, Transforming and Loading the input data as a notebook

The etl python notebook helps think through the ETL process in small steps. In this document, it is not the intention to run the whole ETL pipeline, but to test and validate the approach. The automatic processing "on scale" for the ETL (ETL pipeline) is done in the `etl.py` document, with the learnings from this document `etl.ipynb`. So make sure to check this one first.

The ETL process goes as follows:
1. Create the `songs` table
    - read the `song_data` json files
    - insert the relevant attributes into the `songs` table through an sql query from `sql_queries.py`
2. Create the `artists` table
    - read the `song_data` json files
    - insert the relevant attributes into the `artists` table through an sql query from `sql_queries.py`
3. Create the `time` table
    - read the `log_data` json files
    - insert the relevant attributes into the `time` table through an sql query from `sql_queries.py`
4. Create the `users` table
    - read the `log_data` json files
    - insert the relevant attributes into the `users` table through an sql query from `sql_queries.py`
5. Create the `songplays` table
    - read the `log_data`
    - use the `song`, `artist`, and `length`attributes from the `log_data` to compare rows from the existing `artists` and `songs`tables, with the goal of matching an `artist_id` and `song_id`- for every row
    - insert the relevant attributes (from both data source-types) into the `songplays`table through an sql query from `sql_queries.py`

#### Notes
- Assumption about timestamp origin: When converting the timestamp `ts` column in miliseconds to datetime, we assume the default timestamp origin of 'unix' from the `pandas.to_datetime()` function
- Data checks
    - where it made sense, we inserted data checks, i.e. simple print outs of data properties to double-check expectations vs. output

# etl.py
This document contains the ETL pipeline, which we'll describe in this section.

This file contains two modular functions which perform reads writes and one 'wrapper' function, which takes the modular functions and uses them to process larger amounts of data.

1. The `process_song_file` function is built to read an **individual** `song_data` JSON-file and create both the `songs` and the `artits` tables. Both tables come from the same source data, but their columns are different, since each serves a different need. To illustrate the last point:
    - The `songs`table contains the columns: ("song_id", "title", "artist_id", "year", "duration")
    - The `artists` table contains the columns ("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    - To create the tables, we use the queries `song_table_insert` and `artist_table_insert`, which can be found in the `sql_queries.py` file
    
2. The `process_log_file` function was created separately, since it reads an individual JSON-file from the `log_data`. The process is similar to the former function, with the addition of extra steps for the `time` table and the `songplays` table:
    - For the `time` table we first convert the timestamp in miliseconds to a datetime and split it into useful date information (hour, day, month, etc.)
    - For the `songplays`table we use information from both the `log_data` and `song_data` to match for `artist_id` and `song_id`. However we do not read the `song_data` again, but we make use of the `songs` and `artists` tables created earlier (see `main` function)

3. Where the functions above are made to read only one file and process it, the `process_data` gets all files matching of type JSON within an established path and runs a parameter-function over it. This will be handy to use our first two functions over many files. In addition, it prints to the console the amount of files being processed.

4. In the `main` function we connect to the databse and call the `process_data` function twice. First with the `song_data` and the `process_song_file` function and then the `log_data` and `process_log_data` function.
> Note that in the `main` function, we purposefully call process the `song_data` *before* the `log_data`, since the creation of the `songplays` table involves the `songs` and `artists` tables.

# test.ipynb
This file uses the `%load_ext sql` extension to test the queries given to us by the analytics teams. The queries select data from all tables to check if the tables were created properly and to test if the ETL pipeline filled the tables with the proper data-content.

## How to run the Python scripts
- be sure to have postgres installed
- make sure that no terminal, notebook are running and that all connections to the database are closed
- open the terminal and  navigate to your workspace
- within the terminal
    - run `python sql_queries.py`
    - run `python create_tables.py`
    - run `python etl.py`
    - to validate the output of the etl pipeline, open `test.ipynb` and run all cells.
        - when finished, make sure to reset the kernel from the notebook

## Clarification of Sources and Wording
Clarification: The complete preparation source for this assignment is attributed to Udacity. My contribution is on top of the templates, instructions, documents, etc. provided by the assignment beforehand.

With the term 'assignment' in this document we mean the whole material provided by Udacity for the completion of the assignment. For readability and simplicity we spare the mention of the source at every relevant sentence in this document.

With 'we' I mean only myself as author of the recquired code and text. I use it for readability and as a best practice from my academic writing habits.

## Example queries for song play analysis
These queries should be run after the etl-pipeline has run. They can be find in the `test.ipynb`
- Get the top 5 most active users with the 'free' app version (`user_id`s)
```
%sql SELECT user_id, COUNT(user_id) as song_plays FROM songplays WHERE level = 'free' GROUP BY user_id ORDER BY COUNT(user_id) DESC LIMIT 5;
```
- Get the top 5 most active users with the 'free' app version (`user_id`s)
```
%sql SELECT user_id, COUNT(user_id) as song_plays FROM songplays WHERE level = 'paid' GROUP BY user_id ORDER BY COUNT(user_id) DESC LIMIT 5;
```
- Get the platform where the streaming app is most used from (rough approximation)
```
%sql SELECT LEFT(user_agent, 30) FROM songplays GROUP BY LEFT(user_agent, 30) ORDER BY LEFT(user_agent, 30) DESC LIMIT 10;
```

## Other sources & references:
- https://video.udacity-data.com/topher/2019/October/5d9683fc_dend-p1-lessons-cheat-sheet/dend-p1-lessons-cheat-sheet.pdf
- https://www.tutlane.com/tutorial/sql-server/sql-comparison-operators#:~:text=The%20sql%20not%20equal%20operator%20is%20used%20to,be%20true%20and%20it%20will%20return%20matched%20records
- https://www.postgresql.org/docs/10/sql-copy.html
- https://www.markdownguide.org/cheat-sheet
- https://knowledge.udacity.com/questions/748796

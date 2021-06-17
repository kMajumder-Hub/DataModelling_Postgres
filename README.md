# Project 1: Data modeling with Postgres

This project creates a postgres database sparkifydb for a music app, Sparkify. The purpose of the database is to model song and log datasets (originally stored in JSON format) with a star schema optimised for queries on song play analysis.

## Schema design and ETL pipeline

The star schema has 1 fact table (songplays), and 4 dimension tables (users, songs, artists, time). DROP, CREATE, INSERT, and SELECT queries are defined in sql_queries.py. create_tables.py uses functions create_database, drop_tables, and create_tables to create the database sparkifydb and the required tables.


#### ER Diagram of Data Model
![ER Diagram of Data Model](https://github.com/kaustuv-hub/DataModelling_Postgres/blob/main/erd.PNG)

Extract, transform, load processes in etl.py populate the songs and artists tables with data derived from the JSON song files, data/song_data. 
Processed data derived from the JSON log files, data/log_data, is used to populate time and users tables. 
A SELECT query collects song and artist id from the songs and artists tables and combines this with log file derived data to populate the songplays fact table.


#### Execution and Data Analysis

In order to execute project the executor.ipynb cells need to be executed. There are two cells in this notebook - first to run the create_tables.py script, and then etl.py script.
Proper log will be printed for each successfull file processing

data_analysis.ipynb notebook contains necessary sql statements and python codes to perform basic visualizations of the extracted data
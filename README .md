# Data Warehouse for a music streaming platform

The startup has currently saved the data from its app locally in a SQL-Database.
They moved it to a AWS S3 storage so all app data are now on a S3 server.
As the startup grow a database now no make sense so we move to the Data Warehouse system
from AWS and setup a Redshift Data Warehouse.
So that your Data team can still quickly extract the data and analyze the behaviors of the users
from past data and the actual data.

## Why I choice the AWS Redshift Data Warehouse system

The APP generate always the same data to the S3 storage.
New data can be load easy into the new Data Warehouse system
also the data team can access and manipulate the data with simple SQL queries.

## How to use and explain the files

* Fill the ```dwh.cfg``` with your AWS Redshift and user data.

* Run the ```create_tables``` to create the database and the tables,
 that are located in the ```sql_queries.py``` file.

* Run the ```etl.py``` to load the data from the S3 bucket into the SQL tables

## The Star schema of the database

In this schema for the Sparkify Data Warehouse is the fact table songplays and it has four dimension tables:

* songplays were stored all the data and some facts
 (song_id, user_id, artist_id, ...) Primary Key is songplay_id the Reference Keys are the
 Primary keys from the dimension tables

* time were stored all time data (hour, day, month, ...) Primary Key is start_time

* users were stored all user data (first name, last name, ...) Primary Key is user_id

* songs were stored all song data (title, year, ...) Primary key is song_id

* artist were stored all artist data (name, location, ...) Primary Key is artist_id

![Sparkify Database Schema](sparkify_schem.png)

# Data Warehouse for a music streaming platform

The startup has currently saved the data from its app locally in a SQL-Database.
As the startup grow a locally database now no make sense so we move to the Data Warehouse system
from AWS and setup a Redshift Data Warehouse.
So that your Data team can still quickly extract the data and analyze the behaviors of the users.

## Why I choice the AWS Redshift Data Warehouse system

The APP generate always the same data
New data can be load easy into the new Data Warehouse
also the data team can access and manipulate the data with simple SQL queries.

## How to use and explain the files

* Fill the ```dwh.cfg``` with your AWS Redshift and user data.

* Run the ```create_tables``` to create the database and the tables, that are located in the ```sql_queries.py``` file.

* Run the ```etl.py``` to load the data from the S3 bucket into the SQL tables

## The Star schema of the database

In this schema for the Sparkify DB is the fact table songplays and it has four dimension tables named:

* time were stored all time data (hour, day, month, ...)

* users were stored all user data (first name, last name, ...)

* songs were stored all song data (title, year, ...)

* artist were stored all artist data (name, location, ...)

![Sparkify Database Schema](sparkify_schem.png)

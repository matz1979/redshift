import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    read in both staging_tables from the AWS S3 storage
    arg {
        :cur = open db connection cursor
        :conn = new db connection
        }
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Insert the values from the staging_tables into the fact and dimension tables
    arg {
        :cur = open db connection cursor
        :conn = new db connection
        }
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # read in the dwh.cfg file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # connect to the database/redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # call the functions load_staging_tables and insert_tables
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    
    #close the connection
    conn.close()


if __name__ == "__main__":
    main()
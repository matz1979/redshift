import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    ''' 
    Call the DROP Tables statement from the sql_queries.py and execute it
    arg {
        :cur = connection cursor
        :conn = connection db
    }
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Call the CREATE Tables statement from the sql_queries.py and execute it
    arg {
        :cur = connection cursor
        :conn = connection db
    }
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    #read in the dhw.cfg file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    #connect to the database/redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    #close the connection
    conn.close()


if __name__ == "__main__":
    main()
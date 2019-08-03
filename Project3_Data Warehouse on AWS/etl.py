import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

"""the queries below to load data from S3 buckets
to AWS Redshift
"""
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print('Loading data : '+query)
        cur.execute(query)
        conn.commit()

"""using INSERT statements to insert data from staging tables to 
the dimension and fact tables that are created before
"""
def insert_tables(cur, conn):
    for query in insert_table_queries:
        print('Transform data: '+query)
        cur.execute(query)
        conn.commit()



def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
  
    print('Connecting to Redshift: ')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))

    print('Connected to Redshift')
    cur = conn.cursor()
    
    print('Loading staging tables: ')
    load_staging_tables(cur, conn)
    
    print('Transforming data from staging: ')
    insert_tables(cur, conn)

    conn.close()
    print('ETL process ended, close the connection')


if __name__ == "__main__":
    main()
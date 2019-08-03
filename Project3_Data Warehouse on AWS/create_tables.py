import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

"""using drop_table_queries to drop database tables, 
the DROP statements are listed the list of tables, make sure to drop table before loading them
"""

def drop_tables(cur, conn):
    for query in drop_table_queries:
        print('Executing drop: '+query)
        cur.execute(query)
        conn.commit()


""" In create_table_queries there is a list with CREATE statements
to create new tables
"""
def create_tables(cur, conn):
    for query in create_table_queries:
        print('Executing create: '+query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('Connecting to redshift')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('Connected to redshift')
    cur = conn.cursor()

    print('Dropping existing tables if any')
    drop_tables(cur, conn)
    
    print('Creating tables')
    create_tables(cur, conn)

    conn.close()
    print('Create table Ended')


if __name__ == "__main__":
    main()
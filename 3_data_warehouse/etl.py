import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    - Runs all copy-statements as given through imported sql-queries (copy_table_queries)
    
    - The drop sql-queries are imported from 'sql_queries.py'
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    - Runs all insert-statements as given through imported sql-queries (insert_table_queries)
    
    - The drop sql-queries are imported from 'sql_queries.py'
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Connects with the given database and creates a cursor to read/write on it
    
    - Runs the functions 'load_staging_tables' and 'insert_tables' using the connection and the cursor
    
    - Finally, closes the connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
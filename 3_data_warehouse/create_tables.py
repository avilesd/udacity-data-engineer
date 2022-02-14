import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    - Drops all tables if they exist using imported sql-queries (drop_table_queries)
    
    - The drop sql-queries are imported from 'sql_queries.py'
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    - Creates all tables as given through imported sql-queries (create_tables)
    
    - The drop sql-queries are imported from 'sql_queries.py'
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Connects with the given database and creates a cursor to read/write on it
    
    - Runs the functions 'drop_tables' and 'create_tables' using the connection and the cursor
    
    - Finally, closes the connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
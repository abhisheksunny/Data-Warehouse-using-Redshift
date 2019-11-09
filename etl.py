import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """This will load song_data and log_data partioned JSON file to Redshift as intermediate tables."""
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e: 
            print(str(e)+"\n"+query)
            return

def insert_tables(cur, conn):
    """This will load data from the staging tables to final tables for further use."""
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e: 
            print(str(e)+"\n"+query)
            return


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    print("Staging Table loading - Done.")
    insert_tables(cur, conn)
    print("Table data insertion - Done.")

    conn.close()


if __name__ == "__main__":
    main()
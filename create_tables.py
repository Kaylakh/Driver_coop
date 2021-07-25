import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    # connect to default database
    conn = psycopg2.connect(host="127.0.0.1", database="postgres", user="postgres", password="1234")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create tdc database
    cur.execute("DROP DATABASE IF EXISTS tdcdb")
    cur.execute("CREATE DATABASE tdcdb")

    # close connection to default database
    conn.close()

    # connect to tdcdb database
    conn = psycopg2.connect(host="127.0.0.1", database="tdcdb", user="postgres", password="1234")
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def postgres_connect():
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


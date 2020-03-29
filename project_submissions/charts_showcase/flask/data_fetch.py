import pandas
import sqlite3
import config


def db_connect():
    conn = sqlite3.connect(config.DB_PATH)
    return conn


def db_disconnect(conn):
    conn.close()


def db_read_to_df(conn, query):
    df = pandas.read_sql_query(query, conn)
    return df


def db_read(query, size):
    conn = db_connect()
    df = db_read_to_df(conn, query)
    db_disconnect(conn)
    return df.head(size)

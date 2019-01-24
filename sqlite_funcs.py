import datetime
import sqlite3
from sqlite3 import Error
import itertools
import os

#Some of this was written by Sebastian Raschka -> https://sebastianraschka.com/Articles/2014_sqlite_in_python_tutorial.html#inserting-and-updating-rows
#others were taken from SQLite3 tutorials
#Thank you all

def create_new_database(db_file):
    """ If you need to create a new database use this function
    This function creates a database connection to a SQLite database """
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        conn.close()

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn

def see_all_tables(conn):
    """See all tables in your current database"""
    curs = conn.cursor()
    curs.execute("""
    SELECT 
    name 
    FROM sqlite_master 
    WHERE 
    type='table'""")
    
    return curs.fetchall()

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def drop_tables(conn, table):
    curs = conn.cursor()
    curs.execute(f"DROP TABLE {table}")
    print(curs.fetchall())
    
def insert_records(conn, insert_statement, fields):
    """This function insert records into a database"""
    curs = conn.cursor()
    curs.executemany(insert_statement, fields)
    curs.close()
    
    return print(f'There have been {len(fields)} records inserted')

def table_col_info(conn, table_name, print_out=False):
    """ Returns a list of tuples with column informations:
    (id, name, type, notnull, default_value, primary_key)
    """
    curs = conn.cursor()
    curs.execute(f'PRAGMA TABLE_INFO({table_name})')
    info = curs.fetchall()
    conn.close()

    if print_out:
        print("\nColumn Info:\nID, Name, Type, NotNull, DefaultVal, PrimaryKey")
        for col in info:
            print(col)
    return info
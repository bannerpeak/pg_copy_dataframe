#!/usr/bin/env python

# coding: utf-8

"""
Utility to copy pandas dataframe to PostgreSQL.
"""

import ConfigParser
import cStringIO

import pandas as pd
from sqlalchemy import create_engine, MetaData

def get_connection_str(db='db', setup_file='setup.cfg'):
    """
    Get database connection string from configuration variables.
    """

    config = ConfigParser.ConfigParser()
    config.read(setup_file)
    connection_str = config.get(db, 'connection_string')

    return connection_str


def db_connect(connection_str, get_raw_con=True):
    """
    Set up SQLalchemy database connection.
    """

    engine = create_engine(connection_str, isolation_level='AUTOCOMMIT')

    con = engine.connect()

    raw_con = None
    if get_raw_con:
        raw_con = engine.raw_connection()

    metadata = MetaData(engine)

    return engine, metadata, con, raw_con


def copy_from_df(raw_con, df, table_name, sep='\t', copy_cols=None):
    """
    Stream pandas dataframe to cStringIO and use sqlalchemy raw_connection and psycopg2
    copy_expert function.
    """

    in_memory_file = cStringIO.StringIO()

    if copy_cols:
        df = df[copy_cols]

    df.to_csv(in_memory_file,
              sep=sep,
              float_format='%0.3f',
              date_format='%Y-%m-%d',
              index=False,
              encoding='utf-8'
              )

    in_memory_file.seek(0)  # rewind file

    cur = raw_con.cursor()

    COLUMN_LIST_STRING = ','.join(list(df.columns))

    SQL_STATEMENT = """COPY {} ({}) FROM STDIN WITH DELIMITER E'{}' CSV HEADER"""\
                    .format(table_name, COLUMN_LIST_STRING, sep)

    cur.copy_expert(sql=SQL_STATEMENT, file=in_memory_file)

    raw_con.commit()

    cur.close()


if __name__ == '__main__':
    # create dataframe with same columns as expected PostgreSQL destination table
    df = pd.DataFrame({'a': {0: 'x', 1: 'y', 2: 'z'},
                       'b': {0: 1, 1: 3, 2: 5},
                       'c': {0: 2, 1: 4, 2: 6}}
                      )

    # connect to PostgreSQL via sqlalchemy
    connection_str = get_connection_str(db='db', setup_file='config.cfg')
    engine, metadata, con, raw_con = db_connect(connection_str, get_raw_con=True)

    # copy dataframe to PostgreSQL
    copy_from_df(raw_con, df, 'destination_table', sep='\t', copy_cols=['a', 'b'])

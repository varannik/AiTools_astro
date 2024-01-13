#!/usr/bin/python
import psycopg2
from configparser import ConfigParser
import os


def checkConfExist(path):

    return os.path.isfile(path)


def config(section='postgresql'):
    # create a parser
    parser = ConfigParser()
    filename = '/usr/local/airflow/plugins/modules/database.ini'
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


def db_connector(func):
    def with_connection_(*args,**kwargs):

        conn = None
        # read connection parameters
        # params = config()
        try:
            print('Connecting to the db...')

            conn = psycopg2.connect(
                                    host="172.19.0.2",
                                    database="aitools",
                                    user="varanik",
                                    password="Wsxokn2190",
                                    port=5432)
            # conn = psycopg2.connect(**params)
            rv = func(conn, *args,**kwargs)

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        else:
            conn.commit()
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')

        return rv
    return with_connection_
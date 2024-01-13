"""
Create all raw tables if not exist
"""

from db import db_connector
import pandas as pd
import psycopg2.extras as extras
from psycopg2.sql import Identifier, SQL , Placeholder


@db_connector
def cretaTables(cnn, tables):
  '''Create table from dict values'''

  cur = cnn.cursor()

  print("Creatig tabales...")
  print("--------------")
  for k,v in tables.items():
    SQL = f'CREATE TABLE IF NOT EXISTS  "DW_RAW"."{k}" ({v})'
    print(k)
    cur.execute(SQL)
  print("--------------")
  print("Done.")


@db_connector
def insertData(cnn, df, table):
  '''Insert data to db'''

  cur = cnn.cursor()
  # Create a list of tupples from the dataframe values
  tuples = [tuple(x) for x in df.to_numpy()]
  print(tuples)
  # Comma-separated dataframe columns
  cols = ','.join(list(df.columns))
  query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
  print("Insert data to db...")
  print("--------------")
  extras.execute_values(cur, query, tuples)
  print("--------------")
  print(f"{len(df)} row have just loaded")
  print("Done.")


@db_connector
def fetchData(cnn, tableName , columns):
  '''Fetch data form db to Dataframe'''

  query = "SELECT " + ",".join(columns) + f" FROM {tableName}"
  cur = cnn.cursor()
  cur.execute(query)


  print("Fetching data from db...")
  print("--------------")
  tuples_list = cur.fetchall()
  df = pd.DataFrame(tuples_list, columns=columns)
  print("--------------")
  print("Done.")

  # Now we need to transform the list into a pandas DataFrame:

  return df



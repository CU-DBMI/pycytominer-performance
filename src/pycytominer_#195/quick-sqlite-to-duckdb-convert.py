import sqlite3

import duckdb
import pandas as pd

# create connections for sqlite and duckdb
duckdb_conn = duckdb.connect(database="SQ00014613.duckdb")
sqlite_conn = sqlite3.connect(database="SQ00014613.sqlite")

# export data from sqlite tables to duckdb database
for table in pd.read_sql(
    # special note: sqlite_master appears to be required over sqlite_schema sometimes...
    # reference: https://stackoverflow.com/questions/70418785/sqlite3-operationalerror-no-such-table-sqlite-schema
    sql="SELECT name FROM sqlite_master WHERE type ='table'",
    con=sqlite_conn,
)["name"].values.tolist():
    df = pd.read_sql(sql=f"select * from {table}", con=sqlite_conn)
    duckdb_conn.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df")

# close connections
sqlite_conn.close()
duckdb_conn.close()

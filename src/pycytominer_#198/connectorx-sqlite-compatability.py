"""
ConnectorX SQLite Compatibility Conversion

Remove notnull constraint, replace 'nan' with NULL's,
as experiment towards compatibility.
"""

import pandas as pd
from sqlalchemy import create_engine

db_filename = "SQ00014613.sqlite"
sqlite_err_conn = create_engine(f"sqlite:///{db_filename}").connect()

sql_stmt = """
SELECT name, sql FROM sqlite_master WHERE type ='table'
"""
df = pd.read_sql(sql=sql_stmt, con=sqlite_err_conn).to_dict(orient="records")
for table in df:
    table_name = table["name"]
    table_sql = table["sql"]
    print(f"creating new table for {table_name}")
    # alter to rename the original table with prefix
    sqlite_err_conn.execute(
        statement=f"alter table {table_name} rename to orig_{table_name}"
    )
    # create new table with the original's name and removing all not null constraints
    sqlite_err_conn.execute(statement=table_sql.replace("NOT NULL", ""))
    # copy data from original to new table
    sqlite_err_conn.execute(
        statement=f"insert into {table_name} select * from orig_{table_name}"
    )
    # delete original table
    sqlite_err_conn.execute(statement=f"drop table orig_{table_name}")

    # gather cols from table
    col_names = pd.read_sql(
        sql=f"select name from pragma_table_info('{table_name}')", con=sqlite_err_conn
    )["name"].values.tolist()

    for col in col_names:
        # update 'nan' values to NULL
        sqlite_err_conn.execute(
            statement=f"update {table_name} set {col}=NULL where {col}='nan'"
        )

print("all finished")

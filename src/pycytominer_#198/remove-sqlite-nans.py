import sqlite3

import pandas as pd

# create connections for sqlite
# reference: https://nih.figshare.com/articles/dataset/Cell_Health_-_Cell_Painting_Single_Cell_Profiles/9995672
sqlite_conn = sqlite3.connect("mod_SQ00014613.sqlite")

image_cols = pd.read_sql("PRAGMA table_info(Image);", con=sqlite_conn)
cells_cols = pd.read_sql("PRAGMA table_info(Cells);", con=sqlite_conn)
cyto_cols = pd.read_sql("PRAGMA table_info(Cytoplasm);", con=sqlite_conn)
nuclei_cols = pd.read_sql("PRAGMA table_info(Nuclei);", con=sqlite_conn)

df_dict = {
    "image": image_cols,
    "cells": cells_cols,
    "cytoplasm": cyto_cols,
    "nuclei": nuclei_cols,
}

for tabname, df in df_dict.items():
    for colname in df[df["type"].isin(["FLOAT", "BIGINT"])]["name"].values.tolist():
        sql = f"UPDATE {tabname} SET {colname} = replace({colname}, 'nan', 0);"
        sqlite_conn.execute(sql)
        sqlite_conn.commit()

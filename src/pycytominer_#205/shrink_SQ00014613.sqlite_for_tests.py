"""
Creates small test dataset from SQ00014613.sqlite for testing
for cytomining/pycytominer#205.

Source:
https://nih.figshare.com/articles/dataset/
Cell_Health_-_Cell_Painting_Single_Cell_Profiles/9995672?file=18506036
"""

import shutil
import sqlite3

sqlite_source = "SQ00014613.sqlite"
sqlite_target = "test_SQ00014613.sqlite"

shutil.copy(sqlite_source, sqlite_target)

sqlite_conn = sqlite3.connect(sqlite_target)

delete_statement = """
DELETE FROM Image 
WHERE TableNumber NOT IN 
('dd77885d07028e67dc9bcaaba4df34c6',
'1e5d8facac7508cfd4086f3e3e950182')
"""
sqlite_conn.execute(delete_statement)
sqlite_conn.commit()

delete_statement = """
DELETE FROM Cytoplasm 
WHERE TableNumber NOT IN (SELECT TableNumber FROM Image)
OR ObjectNumber > 1
"""
sqlite_conn.execute(delete_statement)

delete_statement = """
DELETE FROM Nuclei 
WHERE TableNumber NOT IN (SELECT TableNumber FROM Image)
OR ObjectNumber > 1
"""
sqlite_conn.execute(delete_statement)

delete_statement = """
DELETE FROM Cells 
WHERE TableNumber NOT IN (SELECT TableNumber FROM Image)
OR ObjectNumber > 1
"""
sqlite_conn.execute(delete_statement)

sqlite_conn.commit()

sqlite_conn.execute("VACUUM")

sqlite_conn.commit()

# close connections
sqlite_conn.close()

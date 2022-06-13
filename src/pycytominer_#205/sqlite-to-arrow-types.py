import tempfile
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
import connectorx as cx


def database_engine_for_testing() -> Engine:
    """
    A database engine for testing as a fixture to be passed
    to other tests within this file.
    """

    # get temporary directory
    tmpdir = tempfile.gettempdir()

    # create a temporary sqlite connection
    sql_path = f"sqlite:///{tmpdir}/test_sqlite.sqlite"
    engine = create_engine(sql_path)

    # statements for creating database with simple structure
    create_stmts = [
        "drop table if exists tbl_a;",
        """
        create table tbl_a (
        col_integer INTEGER NOT NULL
        ,col_text TEXT
        ,col_blob BLOB
        ,col_real REAL
        ,col_null TEXT
        );
        """,
    ]

    insert_vals = [1, "sample", b"sample_blob", 0.5, None]

    with engine.begin() as connection:
        for stmt in create_stmts:
            connection.execute(stmt)

        # insert statement with some simple values
        # note: we use SQLAlchemy's parameters to insert data properly, esp. BLOB
        connection.execute(
            (
                "INSERT INTO tbl_a (col_integer, col_text, col_blob, col_real, col_null)"
                "VALUES (?, ?, ?, ?, ?);"
            ),
            insert_vals,
        )

    return engine


engine = database_engine_for_testing()
url = str(engine.url)
sql_stmt = """
select * from tbl_a;
"""
table = cx.read_sql(conn=url, query=sql_stmt, return_type="arrow")
print(type(table))
print(table.schema)
print(table)

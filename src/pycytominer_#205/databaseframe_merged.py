"""
DatabaseFrame class for extracting data as similar
collection of in-memory data
"""
import os
import tempfile
from typing import Optional

import connectorx as cx
import pyarrow as pa
import ray
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

ray.init()


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
        "drop table if exists Image;",
        """
        create table Image (
        TableNumber INTEGER
        ,ImageNumber INTEGER
        ,ImageData INTEGER
        );
        """,
        "drop table if exists Cells;",
        """
        create table Cells (
        TableNumber INTEGER
        ,ImageNumber INTEGER
        ,ObjectNumber INTEGER
        ,CellsData INTEGER
        );
        """,
        "drop table if exists Nucleus;",
        """
        create table Nucleus (
        TableNumber INTEGER
        ,ImageNumber INTEGER
        ,ObjectNumber INTEGER
        ,NucleusData INTEGER
        );
        """,
        "drop table if exists Cytoplasm;",
        """
        create table Cytoplasm (
        TableNumber INTEGER
        ,ImageNumber INTEGER
        ,ObjectNumber INTEGER
        ,Cytoplasm_Parent_Cells INTEGER
        ,Cytoplasm_Parent_Nuclei INTEGER
        ,CytoplasmData INTEGER
        );
        """,
    ]

    with engine.begin() as connection:
        for stmt in create_stmts:
            connection.execute(stmt)

        # images
        connection.execute(
            "INSERT INTO Image VALUES (?, ?, ?);"[1, 1, 1],
        )

        # cells
        connection.execute(
            "INSERT INTO Cells VALUES (?, ?, ?, ?);"[1, 1, 2, 1],
        )
        connection.execute(
            "INSERT INTO Cells VALUES (?, ?, ?, ?);"[1, 1, 3, 1],
        )

        # nucleus
        connection.execute(
            "INSERT INTO Nucleus VALUES (?, ?, ?, ?);"[1, 1, 4, 1],
        )
        connection.execute(
            "INSERT INTO Nucleus VALUES (?, ?, ?, ?);"[1, 1, 5, 1],
        )

        # cytoplasm
        connection.execute(
            "INSERT INTO Cytoplasm VALUES (?, ?, ?, ?, ?, ?);"[1, 1, 6, 2, 4, 1],
        )
        connection.execute(
            "INSERT INTO Cytoplasm VALUES (?, ?, ?, ?, ?, ?);"[1, 1, 7, 3, 5, 1],
        )

    return engine


@ray.remote
class DatabaseFrame:
    """
    Create a scalable in-memory dataset from
    all tables within provided database.
    """

    def __init__(self, engine: str) -> None:
        self.engine = self.engine_from_str(sql_engine=engine)
        self.arrow_data = {}

    @staticmethod
    def engine_from_str(sql_engine: str) -> Engine:
        """
        Helper function to create engine from a string.

        Parameters
        ----------
        sql_engine: str
            filename of the SQLite database

        Returns
        -------
        sqlalchemy.engine.base.Engine
            A SQLAlchemy engine
        """

        # if we don't already have the sqlite filestring, add it
        if "sqlite:///" not in sql_engine:
            sql_engine = f"sqlite:///{sql_engine}"
        engine = create_engine(sql_engine)

        return engine

    def collect_sql_tables(
        self,
        table_name: Optional[str] = None,
    ) -> list:
        """
        Collect a list of tables from the given engine's
        database using optional table specification.

        Parameters
        ----------
        table_name: str
            optional specific table name to check within database, by default None

        Returns
        -------
        list
            Returns list, and if populated, contains tuples with values
            similar to the following. These may also be accessed by name
            similar to dictionaries, as they are SQLAlchemy Row objects.
            [('table_name'),...]
        """

        # create column list for return result
        table_list = []

        with self.engine.connect() as connection:
            if table_name is None:
                # if no table name is provided, we assume all tables must be scanned
                table_list = connection.execute(
                    "SELECT name as table_name FROM sqlite_master WHERE type = 'table';"
                ).fetchall()
            else:
                # otherwise we will focus on just the table name provided
                table_list = [{"table_name": table_name}]

        return table_list

    def collect_sql_columns(
        self,
        table_name: Optional[str] = None,
        column_name: Optional[str] = None,
    ) -> list:
        """
        Collect a list of columns from the given engine's
        database using optional table or column level
        specification.

        Parameters
        ----------
        table_name: str
            optional specific table name to check within database, by default None
        column_name: str
            optional specific column name to check within database, by default None

        Returns
        -------
        list
            Returns list, and if populated, contains tuples with values
            similar to the following. These may also be accessed by name
            similar to dictionaries, as they are SQLAlchemy Row objects.
            [('table_name', 'column_name', 'column_type', 'notnull'),...]
        """

        # create column list for return result
        column_list = []

        tables_list = self.collect_sql_tables(table_name=table_name)

        with self.engine.connect() as connection:
            for table in tables_list:

                # if no column name is specified we will focus on all columns within the table
                sql_stmt = """
                SELECT :table_name as table_name,
                        name as column_name,
                        type as column_type,
                        [notnull]
                FROM pragma_table_info(:table_name)
                """

                if column_name is not None:
                    # otherwise we will focus on only the column name provided
                    sql_stmt = f"{sql_stmt} WHERE name = :col_name;"

                # append to column list the results
                column_list += connection.execute(
                    sql_stmt,
                    {
                        "table_name": str(table["table_name"]),
                        "col_name": str(column_name),
                    },
                ).fetchall()

        return column_list

    def sql_table_to_arrow_table(
        self,
        table_name: Optional[str] = None,
    ) -> pa.Table:
        """
        Read provided table as PyArrow Table

        Parameters
        ----------
        table_name: str
            optional specific table name to check within database, by default None

        Returns
        -------
        pyarrow.Table
            PyArrow Table of the SQL table
        """

        return cx.read_sql(
            str(self.engine.url), f"select * from {table_name};", return_type="arrow"
        )

    def collect_arrow_tables(
        self,
        table_name: Optional[str] = None,
    ) -> dict:
        """
        Collect all tables within class's provided engine
        as PyArrow Tables.

        Parameters
        ----------
        table_name: str
            optional specific table name to check within database, by default None

        Returns
        -------
        dict
            dictionary of PyArrow Table(s) from the SQL table(s)
        """

        # for each table in the database gather an arrow table and
        # organize within dictionary.

        self.arrow_data = {}

        for table in self.collect_sql_tables(table_name=table_name):
            self.arrow_data[table["table_name"]] = self.sql_table_to_arrow_table(
                table_name=table["table_name"]
            )

        return self.arrow_data

    def collect_ray_datasets(
        self,
        table_name: Optional[str] = None,
    ) -> dict:
        """
        Collect all tables within class's provided engine
        as Ray Datasets.

        Parameters
        ----------
        table_name: str
            optional specific table name to check within database, by default None

        Returns
        -------
        dict
            dictionary of Ray Dataset(s) from the SQL table(s)
        """

        self.ray_data = {}

        # collect tables as arrow data if we haven't already
        if len(self.arrow_data) == 0:
            self.collect_arrow_tables(table_name=table_name)

        for arr_table_name, arr_table_data in self.arrow_data.items():
            self.ray_data[arr_table_name] = ray.data.from_arrow(arr_table_data)

        return self.ray_data

    def merged(
        self,
    ) -> ray.data.Dataset:
        """
        Create merged format

        Returns
        -------
        ray.data.Dataset
            Single merged dataset
        """

        # collect tables as ray data if we haven't already
        if len(self.ray_data) == 0:
            self.collect_ray_tables(table_name=table_name)

        # presumes available image, cells, nucleus, and cytoplasm tables

        # rename objectnumber for each biological compartment to avoid overlap
        # join cytoplasm full outer
        # join cells full outer
        # join nucleus full outer

        return self.ray_data

    def to_parquet(
        self,
        table_name: Optional[str] = None,
        path: Optional[str] = None,
    ) -> dict:
        """
        Collect all tables within class's provided engine
        as Ray Datasets.

        Parameters
        ----------
        table_name: str
            optional specific table name to check within database, by default None
        path: str
            optional path for directory where parquet output shall be placed

        Returns
        -------
        dict
            dictionary of parquet file locations by table name
        """

        self.parquet_data = {}

        # if our path is none, create a default path using the basename of the engine url
        if path is None:
            path = f"./{os.path.splitext(os.path.basename(str(self.engine.url)))[0]}"

        # collect tables as ray datasets if we haven't already
        if len(self.arrow_data) == 0:
            self.collect_ray_datasets(table_name=table_name)

        for ray_dataset_name, ray_dataset_data in self.ray_data.items():
            self.parquet_data[ray_dataset_name] = ray_dataset_data.write_parquet(
                path=path
            )

        return self.parquet_data


dbf = DatabaseFrame.remote(engine=str(database_engine_for_testing().url))
arrow_tables = ray.get(dbf.collect_arrow_tables.remote())
print(arrow_tables)
ray_datasets = ray.get(dbf.collect_ray_datasets.remote())
parquet_datasets = ray.get(dbf.to_parquet.remote())

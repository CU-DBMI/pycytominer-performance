"""
DatabaseFrame class for extracting data as similar
collection of in-memory data
"""
import os
import tempfile
from typing import List, Optional

import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import ray
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine


def database_engine_for_testing() -> Engine:
    """
    A database engine for testing as a fixture to be passed
    to other tests within this file.
    """

    # get temporary directory
    tmpdir = tempfile.gettempdir()

    # remove db if it exists
    if os.path.exists(f"{tmpdir}/test_sqlite.sqlite"):
        os.remove(f"{tmpdir}/test_sqlite.sqlite")

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
        ,RandomDate DATETIME
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
        "drop table if exists Nuclei;",
        """
        create table Nuclei (
        TableNumber INTEGER
        ,ImageNumber INTEGER
        ,ObjectNumber INTEGER
        ,NucleiData INTEGER
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
            "INSERT INTO Image VALUES (?, ?, ?, ?);",
            [1, 1, 1, "123-123"],
        )

        # cells
        connection.execute(
            "INSERT INTO Cells VALUES (?, ?, ?, ?);",
            [1, 1, 2, 1],
        )
        connection.execute(
            "INSERT INTO Cells VALUES (?, ?, ?, ?);",
            [1, 1, 3, 1],
        )

        # Nuclei
        connection.execute(
            "INSERT INTO Nuclei VALUES (?, ?, ?, ?);",
            [1, 1, 4, 1],
        )
        connection.execute(
            "INSERT INTO Nuclei VALUES (?, ?, ?, ?);",
            [1, 1, 5, 1],
        )

        # cytoplasm
        connection.execute(
            "INSERT INTO Cytoplasm VALUES (?, ?, ?, ?, ?, ?);",
            [1, 1, 6, 2, 4, 1],
        )
        connection.execute(
            "INSERT INTO Cytoplasm VALUES (?, ?, ?, ?, ?, ?);",
            [1, 1, 7, 3, 5, 1],
        )

    return engine


class DatabaseFrame:
    """
    Create a scalable in-memory dataset from
    all tables within provided database.
    """

    def __init__(
        self,
        engine: str,
        compartments: List[str] = None,
        join_keys: List[str] = None,
    ) -> None:
        self.sql_url = engine
        self.engine = self.engine_from_str(sql_engine=engine)
        self.pandas_data = self.collect_pandas_dataframes()
        self.dataframes_merged = self.to_cytomining_merged(
            compartments=compartments, join_keys=join_keys
        )

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

    def sql_table_to_pd_dataframe(
        self,
        table_name: str,
        prepend_tablename_to_cols: bool = True,
        avoid_prepend_for=List[str],
    ) -> pd.DataFrame:
        """
        Read provided table as pandas dataframe

        Parameters
        ----------
        table_name: str
            specific table name to check within database, by default None
        prepend_tablename_to_cols: bool
            Whether prepend table name to column names, by default true
        avoid_prepend_for: List[str]
            list of strings of column names to avoid prepending the table name to.

        Returns
        -------
        pd.DataFrame
            Pandas Dataframe of the SQL table
        """

        if prepend_tablename_to_cols:
            colnames = [
                coldata["column_name"]
                for coldata in self.collect_sql_columns(table_name=table_name)
            ]
            colstring = ",".join(
                [
                    f"{colname} as '{table_name}_{colname}'"
                    if colname not in avoid_prepend_for
                    else colname
                    for colname in colnames
                ]
            )
            sql_stmt = f"select {colstring} from {table_name}"
        else:
            sql_stmt = f"select * from {table_name}"

        return pd.read_sql(sql_stmt, self.sql_url)

    def collect_pandas_dataframes(
        self,
        table_name: Optional[str] = None,
    ) -> dict:
        """
        Collect all tables within class's provided engine
        as Pandas Dataframes.

        Parameters
        ----------
        table_name: str
            optional specific table name to check within database, by default None

        Returns
        -------
        dict
            dictionary of Pandas Dataframe(s) from the SQL table(s)
        """

        self.pandas_data = {}

        # for each table in the database gather an pandas dataframe and
        # organize within dictionary.
        for table in self.collect_sql_tables(table_name=table_name):
            self.pandas_data[table["table_name"]] = self.sql_table_to_pd_dataframe(
                table_name=table["table_name"],
                prepend_tablename_to_cols=True,
                avoid_prepend_for=["TableNumber", "ImageNumber"],
            )

        return self.pandas_data

    @staticmethod
    def df_name_prepend_column_rename(
        name: str,
        dataframe: pd.DataFrame,
        avoid: List[str],
    ) -> pd.DataFrame:
        """
        Create renamed columns for cytomining efforts

        Parameters
        ----------
        name: str
            name to prepend during rename operation
        dataframe: pd.DataFrame
            table which to perform the column renaming operation
        avoid: List[str]
            list of keys which will be avoided during rename

        Returns
        -------
        pd.DataFrame
            Single dataframe with renamed columns
        """
        dataframe.columns = [
            # prepend table name to the column if the column
            # name is not in the join keys, otherwise leave it
            # for joining operations.
            f"{name}_{x}" if x not in avoid else x
            for x in list(dataframe.columns)
        ]
        return dataframe

    @staticmethod
    def outer_join(
        left: pd.DataFrame,
        right: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Create merged format for cytomining efforts.

        Parameters
        ----------
        left: pd.DataFrame
            left dataframe to join
        right: pd.DataFrame
            right dataframe to join

        Returns
        -------
        pd.Datafame
            Single joined dataset
        """

        return pd.merge(
            left=left,
            right=right,
            on=list(right.columns),
            how="outer",
        )

    def nan_data_fill(self, fill_into: str) -> dict:
        """
        Fill modin dataset with columns of nan's (and set related coltype for compatibility)
        from other tables just once to avoid performance woes.

        See this comment for more detail:
        https://github.com/modin-project/modin/issues/1572#issuecomment-642748842

        Parameters
        ----------
        fill_into: str
            table name to fill na's into

        Returns
        -------
        dict
            dictionary of Pandas Dataframe(s) from the SQL table(s)
        """

        colnames_and_types = {}
        for dataframe_name, dataframe_data in self.pandas_data.items():
            if dataframe_name != fill_into:
                colnames_and_types.update(
                    {
                        colname: str(dataframe_data[colname].dtype).replace(
                            "int64", "float64"
                        )
                        for colname in dataframe_data.columns
                        if colname not in self.pandas_data[fill_into].columns
                    }
                )

        # append all columns not in fill_into table into fill_into
        self.pandas_data[fill_into] = pd.concat(
            [
                self.pandas_data[fill_into],
                pd.DataFrame(
                    {
                        colname: pd.Series(
                            data=np.nan,
                            index=self.pandas_data[fill_into].index,
                            dtype=coltype,
                        )
                        for colname, coltype in colnames_and_types.items()
                    },
                    index=self.pandas_data[fill_into].index,
                ),
            ],
            axis=1,
        )

        return self.pandas_data[fill_into]

    def to_cytomining_merged(
        self,
        compartments: List[str] = None,
        join_keys: List[str] = None,
    ) -> pd.DataFrame:
        """
        Create merged dataset for cytomining efforts.

        Note: presumes the presence of an "Image" table within
        datasets which is used as basis for joining operations.

        Parameters
        ----------
        compartments: List[str]
            list of compartments which will be merged.
            By default Cells, Cytoplasm, Nuclei.
        join_keys: List[str]
            list of keys which will be used for join
            By default TableNumber and ImageNumber.

        Returns
        -------
        pd.DataFrame
            Single merged dataset from compartments provided.
        """

        # set default join_key
        if not join_keys:
            join_keys = ["TableNumber", "ImageNumber"]

        # set default compartments
        if not compartments:
            compartments = ["Cells", "Cytoplasm", "Nuclei"]

        # collect table data if we haven't already
        if len(self.pandas_data) == 0:
            self.pandas_data = self.collect_pandas_dataframes()

        # begin with image as basis
        # prepare image table with columns from other tables for resulting structure needs
        for dataframe in self.pandas_data:
            self.nan_data_fill(fill_into=dataframe)

        self.dataframes_merged = pd.concat(
            [self.pandas_data[name] for name in self.pandas_data]
        )

        return self.dataframes_merged

    def to_parquet(self, filepath: str) -> str:
        """
        Exports merged data content from database
        into parquet file.

        Parameters
        ----------
        filepath: str
            filepath to export to.

        Returns
        -------
        str
            location of parquet filepath
        """

        # export to pandas from ray modin due to support
        self.dataframes_merged.to_parquet(filepath)

        return filepath


dbf = DatabaseFrame(engine=str(database_engine_for_testing().url))
print("\nFinal result\n")
print(dbf)
print(dbf.dataframes_merged)
print(dbf.to_parquet(filepath="./example.parquet"))
print(pd.read_parquet("example.parquet"))

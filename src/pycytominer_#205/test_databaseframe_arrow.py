"""
DatabaseFrame class for extracting data as similar
collection of in-memory data
"""
from typing import List, Optional

import connectorx as cx
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

sql_path = "testing_err_fixed_SQ00014613.sqlite"
sql_url = f"sqlite:///{sql_path}"


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
        self.engine = self.engine_from_str(sql_engine=engine)
        self.arrow_data = self.collect_arrow_tables()
        self.tables_merged = self.to_cytomining_merged(
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

    def sql_table_to_arrow_table(
        self,
        table_name: str,
        prepend_tablename_to_cols: bool = True,
        avoid_prepend_for=List[str],
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

        if prepend_tablename_to_cols:
            colnames = [
                coldata["column_name"]
                if coldata["column_type"] != "DATETIME"
                else "CAST({} AS TEXT)".format(coldata["column_name"])
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

        return cx.read_sql(
            str(self.engine.url).replace("///", "//"),
            sql_stmt,
            return_type="arrow",
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
                table_name=table["table_name"],
                prepend_tablename_to_cols=True,
                avoid_prepend_for=["TableNumber", "ImageNumber"],
            )

        return self.arrow_data

    @staticmethod
    def table_name_prepend_column_rename(
        name: str,
        table: pa.Table,
        avoid: List[str],
    ) -> pa.Table:
        """
        Create renamed columns for cytomining efforts

        Parameters
        ----------
        name: str
            name to prepend during rename operation
        table: pa.Table
            table which to perform the column renaming operation
        avoid: List[str]
            list of keys which will be avoided during rename

        Returns
        -------
        pa.Table
            Single table with renamed columns
        """

        return table.rename_columns(
            [
                # prepend table name to the column if the column
                # name is not in the join keys, otherwise leave it
                # for joining operations.
                f"{name}_{x}" if x not in avoid else x
                for x in list(table.schema.names)
            ]
        )

    @staticmethod
    def outer_join(
        left: pa.Table,
        right: pa.Table,
        join_keys: List[str],
    ) -> pa.Table:
        """
        Create merged format for cytomining efforts.

        Parameters
        ----------
        join_keys: List[str]
            list of keys which will be used for join

        Returns
        -------
        pa.Table
            Single joined dataset
        """

        # prepare for join, adding null columns for any which are not in
        # right table, and which are also not already in the left table.
        for column in right.schema.names:
            if column not in join_keys and column not in left.schema.names:
                left = left.append_column(
                    column,
                    pa.array(
                        [None] * left.num_rows,
                        type=right.schema.field(column).type,
                    ),
                )

        return left.join(
            right,
            right.schema.names,
            right.schema.names,
            "full outer",
        )

    def to_cytomining_merged(
        self,
        compartments: List[str] = None,
        join_keys: List[str] = None,
    ) -> pa.Table:
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
        pa.Table
            Single merged dataset from compartments provided.
        """

        # set default join_key
        if not join_keys:
            join_keys = ["TableNumber", "ImageNumber"]

        # set default compartments
        if not compartments:
            compartments = ["Cells", "Cytoplasm", "Nuclei"]

        # collect table data if we haven't already
        if len(self.arrow_data) == 0:
            self.collect_arrow_tables()

        # begin with image as basis
        # create initial merged dataset with image table and first provided compartment
        self.table_cytomining_merged = self.arrow_data["Image"]

        # complete the remaining merges with provided compartments
        for compartment in compartments:
            self.table_cytomining_merged = self.outer_join(
                left=self.table_cytomining_merged,
                right=self.arrow_data[compartment],
                join_keys=join_keys,
            )

        return self.table_cytomining_merged

    def to_parquet(self, filepath: str) -> str:
        """
        Exports merged arrow data content from database
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

        pq.write_table(self.tables_merged, filepath)

        return filepath


dbf = DatabaseFrame(engine=sql_url)
print("\nFinal result\n")
print(dbf.tables_merged)
print(dbf.to_parquet(filepath="example.parquet"))
print(pq.read_table("example.parquet"))
print(pq.read_table("example.parquet").to_pandas())

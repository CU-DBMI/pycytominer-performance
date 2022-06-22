"""
DatabaseFrame class for extracting data as similar
collection of in-memory data
"""
from typing import List, Optional

import pandas as pd
import numpy as np
import ray
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

ray.init()

sql_path = "testing_err_fixed_SQ00014613.sqlite"
sql_url = f"sqlite:///{sql_path}"


@ray.remote
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
        table_name: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Read provided table as pandas dataframe

        Parameters
        ----------
        table_name: str
            optional specific table name to check within database, by default None

        Returns
        -------
        pd.DataFrame
            Pandas Dataframe of the SQL table
        """

        return pd.read_sql(f"select * from {table_name};", self.engine)

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
                table_name=table["table_name"]
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
        join_keys: List[str],
    ) -> pd.DataFrame:
        """
        Create merged format for cytomining efforts.

        Parameters
        ----------
        join_keys: List[str]
            list of keys which will be used for join

        Returns
        -------
        pd.Datafame
            Single joined dataset
        """

        # prepare for join, adding null columns for any which are not in
        # right table, and which are also not already in the left table.
        left = pd.concat(
            [
                left,
                pd.DataFrame(
                    {
                        colname: pd.Series(
                            data=np.nan,
                            index=left.index,
                            dtype=str(right[colname].dtype).replace("int64", "float64"),
                        )
                        for colname in right.columns
                        if colname not in join_keys and colname not in left.columns
                    },
                    index=left.index,
                ),
            ],
            axis=1,
        )

        return pd.merge(
            left=left,
            right=right,
            on=list(right.columns),
            how="outer",
        )

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
            self.collect_pandas_dataframes()

        # prepend table name for each table column to avoid overlaps
        for table_name, table_data in self.pandas_data.items():

            # only prepare names for compartments we seek to merge
            if table_name in compartments:
                # prepend table name for each column name except the join keys
                self.pandas_data[table_name] = self.df_name_prepend_column_rename(
                    name=table_name,
                    dataframe=table_data,
                    avoid=join_keys,
                )

        # begin with image as basis
        # create initial merged dataset with image table and first provided compartment
        self.df_cytomining_merged = self.outer_join(
            left=self.pandas_data["Image"],
            right=self.pandas_data[compartments[0]],
            join_keys=join_keys,
        )

        # complete the remaining merges with provided compartments
        for compartment in compartments[1:]:
            self.df_cytomining_merged = self.outer_join(
                left=self.df_cytomining_merged,
                right=self.pandas_data[compartment],
                join_keys=join_keys,
            )

        return self.df_cytomining_merged

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

        self.dataframes_merged.to_parquet(filepath)

        return filepath


dbf = DatabaseFrame.remote(engine=sql_url)
print(ray.get((dbf.to_cytomining_merged.remote())))
print(ray.get(dbf.to_parquet.remote(filepath="example.parquet")))
print(pd.read_parquet("example.parquet"))

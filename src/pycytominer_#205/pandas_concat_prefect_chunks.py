"""
Extracting data as similar
collection of in-memory data
"""
import glob
import os
import tempfile
import uuid
from typing import List, Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from prefect import Flow, Parameter, task, unmapped
from prefect.engine.results import LocalResult
from prefect.executors import DaskExecutor, Executor, LocalExecutor
from prefect.storage import Local
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

if __name__ == "__main__":

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
            connection.execute(
                "INSERT INTO Image VALUES (?, ?, ?, ?);",
                [2, 2, 2, "123-123"],
            )

            # cells
            connection.execute(
                "INSERT INTO Cells VALUES (?, ?, ?, ?);",
                [1, 1, 2, 1],
            )
            connection.execute(
                "INSERT INTO Cells VALUES (?, ?, ?, ?);",
                [2, 2, 3, 1],
            )

            # Nuclei
            connection.execute(
                "INSERT INTO Nuclei VALUES (?, ?, ?, ?);",
                [1, 1, 4, 1],
            )
            connection.execute(
                "INSERT INTO Nuclei VALUES (?, ?, ?, ?);",
                [2, 2, 5, 1],
            )

            # cytoplasm
            connection.execute(
                "INSERT INTO Cytoplasm VALUES (?, ?, ?, ?, ?, ?);",
                [1, 1, 6, 2, 4, 1],
            )
            connection.execute(
                "INSERT INTO Cytoplasm VALUES (?, ?, ?, ?, ?, ?);",
                [2, 2, 7, 3, 5, 1],
            )

        return engine

    @task
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
        if type(sql_engine) is not Engine:
            if "sqlite:///" not in sql_engine:
                sql_engine = f"sqlite:///{sql_engine}"
            engine = create_engine(sql_engine)
        else:
            engine = sql_engine

        return engine

    @task
    def collect_sql_tables(
        engine,
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

        with engine_from_str.run(engine).connect() as connection:
            if table_name is None:
                # if no table name is provided, we assume all tables must be scanned
                table_list = connection.execute(
                    "SELECT name as table_name FROM sqlite_master WHERE type = 'table';"
                ).fetchall()
            else:
                # otherwise we will focus on just the table name provided
                table_list = [{"table_name": table_name}]

        return table_list

    @task
    def collect_sql_columns(
        engine,
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

        tables_list = collect_sql_tables.run(
            engine=engine_from_str.run(engine), table_name=table_name
        )

        with engine_from_str.run(engine).connect() as connection:
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

    @task
    def sql_select_distinct_join_basis(
        engine, table_name: str, join_keys: List[str], chunk_size: int
    ) -> list:
        join_keys_str = ", ".join(join_keys)
        sql_stmt = f"""
        select distinct {join_keys_str} from {table_name}
        """
        basis_dicts = pd.read_sql(
            sql_stmt,
            engine_from_str.run(engine),
        ).to_dict(orient="records")
        chunked_basis_dicts = [
            basis_dicts[i : i + chunk_size]
            for i in range(0, len(basis_dicts), chunk_size)
        ]
        return chunked_basis_dicts

    @task
    def sql_table_to_pl_dataframe(
        engine,
        table_name: str,
        prepend_tablename_to_cols: bool = True,
        avoid_prepend_for=List[str],
        basis_list_dicts: list = None,
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
                for coldata in collect_sql_columns.run(
                    engine=engine_from_str.run(engine), table_name=table_name
                )
            ]
            colstring = ",".join(
                [
                    f"{colname} as '{table_name}_{colname}'"
                    if colname not in avoid_prepend_for
                    else colname
                    for colname in colnames
                ]
            )
            sql_stmt = f"select {colstring}"
        else:
            sql_stmt = f"select *"

        sql_stmt += f" from {table_name}"

        if basis_list_dicts:
            basis_dict_str = " OR ".join(
                [
                    f"({where_group})"
                    for where_group in [
                        " AND ".join(
                            [f"{key} = '{val}'" for key, val in list_dict.items()]
                        )
                        for list_dict in basis_list_dicts
                    ]
                ]
            )
            sql_stmt += f" where {basis_dict_str}"

        return pd.read_sql(sql_stmt, engine_from_str.run(engine))

    @task
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

    @task
    def nan_data_fill(fill_into: pd.DataFrame, fill_from: pd.DataFrame) -> dict:
        """
        Fill modin dataset with columns of nan's (and set related coltype for compatibility)
        from other tables just once to avoid performance woes.

        See this comment for more detail:
        https://github.com/modin-project/modin/issues/1572#issuecomment-642748842

        Parameters
        ----------
        fill_into: pd.DataFrame
            dataframe to fill na's into
        fill_into: pd.DataFrame
            dataframe to fill na's from

        Returns
        -------
        dict
            dictionary of Pandas Dataframe(s) from the SQL table(s)
        """

        colnames_and_types = {
            colname: str(fill_from[colname].dtype).replace("int64", "float64")
            for colname in fill_from.columns
            if colname not in fill_into.columns
        }

        # append all columns not in fill_into table into fill_into
        fill_into = pd.concat(
            [
                fill_into,
                pd.DataFrame(
                    {
                        colname: pd.Series(
                            data=np.nan,
                            index=fill_into.index,
                            dtype=coltype,
                        )
                        for colname, coltype in colnames_and_types.items()
                    },
                    index=fill_into.index,
                ),
            ],
            axis=1,
        )

        return fill_into

    @task
    def table_concatenator(
        engine,
        table_list,
        prepend_tablename_to_cols: bool,
        avoid_prepend_for: list,
        basis_list_dicts: list,
    ):
        concatted = pd.DataFrame()
        for table in table_list:
            to_concat = sql_table_to_pl_dataframe.run(
                engine=engine_from_str.run(engine),
                table_name=table["table_name"],
                prepend_tablename_to_cols=prepend_tablename_to_cols,
                avoid_prepend_for=avoid_prepend_for,
                basis_list_dicts=basis_list_dicts,
            )
            if len(concatted) == 0:
                concatted = to_concat
            else:
                concatted = nan_data_fill.run(fill_into=concatted, fill_from=to_concat)
                to_concat = nan_data_fill.run(fill_into=to_concat, fill_from=concatted)
                concatted = pd.concat([concatted, to_concat])

        return concatted

    @task
    def _to_parquet(df: pd.DataFrame, filename: str) -> str:
        file_uuid = str(uuid.uuid4().hex)
        filename_uuid = f"{filename}-{file_uuid}.parquet"
        df.to_parquet(filename_uuid)
        return filename_uuid

    @task
    def multi_to_single_parquet(
        pq_files: str,
        filename: str,
    ):
        full_filename = f"{filename}.parquet"

        if os.path.isfile(full_filename):
            os.remove(full_filename)

        writer = pq.ParquetWriter(full_filename, pq.read_table(pq_files[0]).schema)
        for tbl in pq_files:
            writer.write_table(pq.read_table(tbl))
            os.remove(tbl)

        writer.close()

        return full_filename

    def run_workflow(
        engine: Engine,
        executor: Executor = None,
        basis: str = None,
        compartments: List[str] = None,
        join_keys: List[str] = None,
        chunk_size: int = 50,
        filename: str = None,
    ) -> pd.DataFrame:
        """
        Create merged dataset for cytomining efforts.

        Note: presumes the presence of an "Image" table within
        datasets which is used as basis for joining operations.

        Parameters
        ----------
        basis: str
            basis table for building dataset
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
        if not executor:
            executor = LocalExecutor()

        if not basis:
            basis = "image"

        # set default join_key
        if not join_keys:
            join_keys = ["TableNumber", "ImageNumber"]

        # set default compartments
        if not compartments:
            compartments = ["Cells", "Cytoplasm", "Nuclei"]

        # collect table data if we haven't already
        # if len(pandas_data) == 0:
        #   pandas_data = collect_pandas_dataframes()

        with Flow("to parquet", storage=Local(), result=LocalResult()) as flow:

            param_engine = Parameter("engine", default="")
            param_basis = Parameter("basis", default="image")
            param_join_keys = Parameter(
                "join_keys", default=["TableNumber", "ImageNumber"]
            )
            param_chunk_size = Parameter("chunk_size", default=20)
            param_filename = Parameter("filename", default="example")

            # chunk the dicts so as to create batches
            basis_dicts = sql_select_distinct_join_basis(
                engine=param_engine,
                table_name=param_basis,
                join_keys=param_join_keys,
                chunk_size=param_chunk_size,
            )

            # gather sql tables for concat
            table_list = collect_sql_tables(engine=param_engine)

            # map to gather our concatted/merged pd dataframes
            df_concat = table_concatenator.map(
                engine=unmapped(param_engine),
                table_list=unmapped(table_list),
                prepend_tablename_to_cols=unmapped(True),
                avoid_prepend_for=unmapped(param_join_keys),
                basis_list_dicts=basis_dicts,
            )

            # map to convert from pd dataframes to arrow tables for pq writing
            pq_files = _to_parquet.map(df=df_concat, filename=unmapped(param_filename))

            # reduce to single pq file
            reduced_pq_result = multi_to_single_parquet(
                pq_files=pq_files, filename=param_filename
            )

        state = flow.run(
            executor=executor,
            parameters=dict(
                engine=engine,
                basis=basis,
                join_keys=join_keys,
                chunk_size=chunk_size,
                filename=filename,
            ),
        )

        print(state.result[reduced_pq_result].result)

        return

    print("\nFinal result\n")
    for filename in glob.glob("./data/example*"):
        os.remove(filename)
    executor = DaskExecutor()
    print(
        run_workflow(
            engine=str(database_engine_for_testing().url),
            executor=executor,
            filename="./data/example",
            chunk_size=1,
        )
    )
    print(pd.read_parquet("./data/example.parquet"))

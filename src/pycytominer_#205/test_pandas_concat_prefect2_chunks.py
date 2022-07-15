"""
Extracting data as similar
collection of in-memory data
"""
import glob
import os
import uuid
from typing import List, Optional

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

if __name__ == "__main__":

    sql_path = "testing_err_fixed_SQ00014613.sqlite"
    sql_url = f"sqlite:///{sql_path}"

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

        with engine.connect() as connection:
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

        tables_list = collect_sql_tables(engine=engine, table_name=table_name)

        with engine.connect() as connection:
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

    @flow(task_runner=ConcurrentTaskRunner())
    def sql_select_distinct_join_basis(
        engine, table_name: str, join_keys: List[str], chunk_size: int
    ) -> list:

        join_keys_str = ", ".join(join_keys)

        sql_stmt = f"""
        select distinct {join_keys_str} from {table_name}
        """

        basis_dicts = pd.read_sql(
            sql_stmt,
            engine_from_str(engine).result(),
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
                for coldata in collect_sql_columns(
                    engine=engine_from_str(engine), table_name=table_name
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

        return pd.read_sql(sql_stmt, engine_from_str(engine))

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
    def to_unique_parquet(df: pd.DataFrame, filename: str) -> str:
        file_uuid = str(uuid.uuid4().hex)
        filename_uuid = f"{filename}-{file_uuid}.parquet"
        df.to_parquet(filename_uuid, compression=None)
        return filename_uuid

    @task
    def multi_to_single_parquet(
        pq_files: list,
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

    @flow(task_runner=ConcurrentTaskRunner())
    def flow_chunked_table_concat_to_parquet(
        engine,
        table_list,
        prepend_tablename_to_cols: bool,
        avoid_prepend_for: list,
        basis_list_dicts: list,
        filename: str,
    ):
        concatted = pd.DataFrame()
        for table in table_list:
            to_concat = sql_table_to_pl_dataframe(
                engine=engine_from_str(engine),
                table_name=table["table_name"],
                prepend_tablename_to_cols=prepend_tablename_to_cols,
                avoid_prepend_for=avoid_prepend_for,
                basis_list_dicts=basis_list_dicts,
            )
            if len(concatted) == 0:
                concatted = to_concat
            else:
                concatted = nan_data_fill(fill_into=concatted, fill_from=to_concat)
                to_concat = nan_data_fill(fill_into=to_concat, fill_from=concatted)
                concatted = pd.concat([concatted, to_concat])

        filename_uuid = to_unique_parquet(df=concatted, filename=filename)

        return filename_uuid

    @flow(task_runner=ConcurrentTaskRunner())
    def flow_reduce_tables_to_single_parquet(
        engine, basis, join_keys, chunk_size, filename
    ):

        # chunk the dicts so as to create batches
        basis_dicts = sql_select_distinct_join_basis(
            engine=engine,
            table_name=basis,
            join_keys=join_keys,
            chunk_size=chunk_size,
        )

        engine = engine_from_str(engine)

        # gather sql tables for concat
        table_list = collect_sql_tables(engine=engine, wait_for=[engine])

        pq_files = []
        print(basis_dicts.result())
        for basis_dict in basis_dicts.result():
            pq_files.append(
                flow_chunked_table_concat_to_parquet(
                    engine=engine,
                    table_list=table_list,
                    prepend_tablename_to_cols=True,
                    avoid_prepend_for=join_keys,
                    basis_list_dicts=basis_dict,
                    filename=filename,
                    wait_for=[
                        engine,
                        basis_dicts,
                        table_list,
                    ],
                )
            )

        reduced_pq_result = multi_to_single_parquet(
            pq_files=pq_files, filename=filename
        )

        return reduced_pq_result

    def convert_sqlite_to_parquet(
        engine: Engine,
        basis: str,
        compartments: List[str],
        join_keys: List[str],
        chunk_size: int,
        filename: str,
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

        # collect table data if we haven't already
        # if len(pandas_data) == 0:
        #   pandas_data = collect_pandas_dataframes()

        if __name__ == "__main__":
            filepath = flow_reduce_tables_to_single_parquet(
                engine=engine,
                basis=basis,
                join_keys=join_keys,
                chunk_size=chunk_size,
                filename=filename,
            )

        return filepath

    print("\nFinal result\n")
    for filename in glob.glob("./data/*"):
        os.remove(filename)

    print(
        convert_sqlite_to_parquet(
            engine=sql_url,
            basis="image",
            compartments=["Cells", "Cytoplasm", "Nuclei"],
            join_keys=["TableNumber", "ImageNumber"],
            filename="./data/testing",
            chunk_size=15,
        )
    )
    print(pl.read_parquet("./data/testing.parquet"))

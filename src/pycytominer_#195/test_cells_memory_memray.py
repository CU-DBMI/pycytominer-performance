import os
import random
import sys
import tempfile
import types

import pandas as pd
import pytest
from pycytominer import aggregate, normalize
from pycytominer.cyto_utils import (
    get_default_compartments,
    get_default_linking_cols,
    infer_cp_features,
)
from pycytominer.cyto_utils.cells import SingleCells, _sqlite_strata_conditions
from sqlalchemy import create_engine

# referenced from https://github.com/cytomining/pycytominer/blob/master/pycytominer/tests/test_cyto_utils/test_cells.py
random.seed(123)


def build_random_data(
    compartment="cells",
    ImageNumber=sorted(["x", "y"] * 50),
    TableNumber=sorted(["x_hash", "y_hash"] * 50),
):
    a_feature = random.sample(range(1, 1000), 100)
    b_feature = random.sample(range(1, 1000), 100)
    c_feature = random.sample(range(1, 1000), 100)
    d_feature = random.sample(range(1, 1000), 100)
    data_df = pd.DataFrame(
        {"a": a_feature, "b": b_feature, "c": c_feature, "d": d_feature}
    ).reset_index(drop=True)

    data_df.columns = [
        "{}_{}".format(compartment.capitalize(), x) for x in data_df.columns
    ]

    data_df = data_df.assign(
        ObjectNumber=list(range(1, 51)) * 2,
        ImageNumber=ImageNumber,
        TableNumber=TableNumber,
    )

    return data_df


# Get temporary directory
tmpdir = tempfile.gettempdir()

# Launch a sqlite connection
file = "sqlite:///{}/test.sqlite".format(tmpdir)

test_engine = create_engine(file)
test_conn = test_engine.connect()

# Setup data
cells_df = build_random_data(compartment="cells")
cytoplasm_df = build_random_data(compartment="cytoplasm").assign(
    Cytoplasm_Parent_Cells=(list(range(1, 51)) * 2)[::-1],
    Cytoplasm_Parent_Nuclei=(list(range(1, 51)) * 2)[::-1],
)
nuclei_df = build_random_data(compartment="nuclei")
image_df = pd.DataFrame(
    {
        "TableNumber": ["x_hash", "y_hash"],
        "ImageNumber": ["x", "y"],
        "Metadata_Plate": ["plate", "plate"],
        "Metadata_Well": ["A01", "A02"],
        "Metadata_Site": [1, 1],
    }
)

image_df_additional_features = pd.DataFrame(
    {
        "TableNumber": ["x_hash", "y_hash"],
        "ImageNumber": ["x", "y"],
        "Metadata_Plate": ["plate", "plate"],
        "Metadata_Well": ["A01", "A01"],
        "Metadata_Site": [1, 2],
        "Count_Cells": [50, 50],
        "Granularity_1_Mito": [3.0, 4.0],
        "Texture_Variance_RNA_20_00": [12.0, 14.0],
        "Texture_InfoMeas2_DNA_5_02": [5.0, 1.0],
    }
)

# Ingest data into temporary sqlite file
image_df.to_sql("image", con=test_engine, index=False, if_exists="replace")
cells_df.to_sql("cells", con=test_engine, index=False, if_exists="replace")
cytoplasm_df.to_sql("cytoplasm", con=test_engine, index=False, if_exists="replace")
nuclei_df.to_sql("nuclei", con=test_engine, index=False, if_exists="replace")

# Create a new table with a fourth compartment
new_file = "sqlite:///{}/test_new.sqlite".format(tmpdir)
new_compartment_df = build_random_data(compartment="new")

test_new_engine = create_engine(new_file)
test_new_conn = test_new_engine.connect()

image_df.to_sql("image", con=test_new_engine, index=False, if_exists="replace")
cells_df.to_sql("cells", con=test_new_engine, index=False, if_exists="replace")
new_cytoplasm_df = cytoplasm_df.assign(
    Cytoplasm_Parent_New=(list(range(1, 51)) * 2)[::-1]
)
new_cytoplasm_df.to_sql(
    "cytoplasm", con=test_new_engine, index=False, if_exists="replace"
)
nuclei_df.to_sql("nuclei", con=test_new_engine, index=False, if_exists="replace")
new_compartment_df.to_sql("new", con=test_new_engine, index=False, if_exists="replace")

new_compartments = ["cells", "cytoplasm", "nuclei", "new"]

new_linking_cols = get_default_linking_cols()
new_linking_cols["cytoplasm"]["new"] = "Cytoplasm_Parent_New"
new_linking_cols["new"] = {"cytoplasm": "ObjectNumber"}

# Ingest data with additional image features to temporary sqlite file

image_file = "sqlite:///{}/test_image.sqlite".format(tmpdir)

test_engine_image = create_engine(image_file)
test_conn_image = test_engine_image.connect()

image_df_additional_features.to_sql(
    "image", con=test_engine_image, index=False, if_exists="replace"
)
cells_df.to_sql("cells", con=test_engine_image, index=False, if_exists="replace")
cytoplasm_df.to_sql(
    "cytoplasm", con=test_engine_image, index=False, if_exists="replace"
)
nuclei_df.to_sql("nuclei", con=test_engine_image, index=False, if_exists="replace")

# Setup SingleCells Class
ap = SingleCells(sql_file=file)
ap_subsample = SingleCells(
    sql_file=file,
    subsample_n=2,
    subsampling_random_state=123,
)
ap_new = SingleCells(
    sql_file=new_file,
    load_image_data=False,
    compartments=new_compartments,
    compartment_linking_cols=new_linking_cols,
)

ap_image_all_features = SingleCells(
    sql_file=image_file,
    add_image_features=True,
    image_feature_categories=["Count", "Granularity", "Texture"],
)

ap_image_subset_features = SingleCells(
    sql_file=image_file,
    add_image_features=True,
    image_feature_categories=["Count", "Texture"],
)

ap_image_count = SingleCells(
    sql_file=image_file, add_image_features=True, image_feature_categories=["Count"]
)


# referenced from https://github.com/cytomining/pycytominer/blob/master/pycytominer/cyto_utils/cells.py
def merge_single_cells(
    self,
    compute_subsample=False,
    sc_output_file="none",
    compression_options=None,
    float_format=None,
    single_cell_normalize=False,
    normalize_args=None,
):
    """Given the linking columns, merge single cell data. Normalization is also supported.

    Parameters
    ----------
    compute_subsample : bool, default False
        Whether or not to compute subsample.
    sc_output_file : str, optional
        The name of a file to output.
    compression_options : str, optional
        Compression arguments as input to pandas.to_csv() with pandas version >= 1.2.
    float_format : str, optional
        Decimal precision to use in writing output file.
    single_cell_normalize : bool, default False
        Whether or not to normalize the single cell data.
    normalize_args : dict, optional
        Additional arguments passed as input to pycytominer.normalize().

    Returns
    -------
    pandas.core.frame.DataFrame
        Either a dataframe (if output_file="none") or will write to file.
    """

    # Load the single cell dataframe by merging on the specific linking columns
    sc_df = ""
    linking_check_cols = []
    merge_suffix_rename = []
    for left_compartment in self.compartment_linking_cols:
        for right_compartment in self.compartment_linking_cols[left_compartment]:
            # Make sure only one merge per combination occurs
            linking_check = "-".join(sorted([left_compartment, right_compartment]))
            if linking_check in linking_check_cols:
                continue

            # Specify how to indicate merge suffixes
            merge_suffix = [
                "_{comp_l}".format(comp_l=left_compartment),
                "_{comp_r}".format(comp_r=right_compartment),
            ]
            merge_suffix_rename += merge_suffix
            left_link_col = self.compartment_linking_cols[left_compartment][
                right_compartment
            ]
            right_link_col = self.compartment_linking_cols[right_compartment][
                left_compartment
            ]

            if isinstance(sc_df, str):
                initial_df = self.load_compartment(compartment=left_compartment)

                if compute_subsample:
                    # Sample cells proportionally by self.strata
                    self.get_subsample(df=initial_df, rename_col=False)

                    subset_logic_df = self.subset_data_df.drop(
                        self.image_df.columns, axis="columns"
                    )

                    initial_df = subset_logic_df.merge(
                        initial_df, how="left", on=subset_logic_df.columns.tolist()
                    ).reindex(initial_df.columns, axis="columns")

                sc_df = initial_df.merge(
                    self.load_compartment(compartment=right_compartment),
                    left_on=self.merge_cols + [left_link_col],
                    right_on=self.merge_cols + [right_link_col],
                    suffixes=merge_suffix,
                )
            else:
                sc_df = sc_df.merge(
                    self.load_compartment(compartment=right_compartment),
                    left_on=self.merge_cols + [left_link_col],
                    right_on=self.merge_cols + [right_link_col],
                    suffixes=merge_suffix,
                )

            linking_check_cols.append(linking_check)

    # Add metadata prefix to merged suffixes
    full_merge_suffix_rename = []
    full_merge_suffix_original = []
    for col_name in self.merge_cols + list(self.linking_col_rename.keys()):
        full_merge_suffix_original.append(col_name)
        full_merge_suffix_rename.append("Metadata_{x}".format(x=col_name))

    for col_name in self.merge_cols + list(self.linking_col_rename.keys()):
        for suffix in set(merge_suffix_rename):
            full_merge_suffix_original.append("{x}{y}".format(x=col_name, y=suffix))
            full_merge_suffix_rename.append(
                "Metadata_{x}{y}".format(x=col_name, y=suffix)
            )

    self.full_merge_suffix_rename = dict(
        zip(full_merge_suffix_original, full_merge_suffix_rename)
    )

    # Add image data to single cell dataframe
    if not self.load_image_data:
        self.load_image()
        self.load_image_data = True

    sc_df = (
        self.image_df.merge(sc_df, on=self.merge_cols, how="right")
        .rename(self.linking_col_rename, axis="columns")
        .rename(self.full_merge_suffix_rename, axis="columns")
    )
    if single_cell_normalize:
        # Infering features is tricky with non-canonical data
        if normalize_args is None:
            normalize_args = {}
            features = infer_cp_features(sc_df, compartments=self.compartments)
        elif "features" not in normalize_args:
            features = infer_cp_features(sc_df, compartments=self.compartments)
        elif normalize_args["features"] == "infer":
            features = infer_cp_features(sc_df, compartments=self.compartments)
        else:
            features = normalize_args["features"]

        normalize_args["features"] = features

        sc_df = normalize(profiles=sc_df, **normalize_args)

    if sc_output_file != "none":
        output(
            df=sc_df,
            output_filename=sc_output_file,
            compression_options=compression_options,
            float_format=float_format,
        )
    else:
        return sc_df


# referenced from https://github.com/cytomining/pycytominer/blob/master/pycytominer/cyto_utils/cells.py
def new_load_compartment(self, compartment):
    """Creates the compartment dataframe.

    Parameters
    ----------
    compartment : str
        The compartment to process.

    Returns
    -------
    pandas.core.frame.DataFrame
        Compartment dataframe.
    """
    compartment_query = "select * from {}".format(compartment)  # nosec
    df = pd.read_sql(sql=compartment_query, con=self.conn)
    return df


def mem_profile_func():
    """
    wrapper function for memory profiling
    """

    # load new_load_compartment as ap's load_compartment function for profiling
    ap.load_compartment = types.MethodType(new_load_compartment, ap)
    return merge_single_cells(self=ap)


mem_profile_func()

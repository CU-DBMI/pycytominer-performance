import os
import types
import numpy as np
import pandas as pd
from pycytominer import aggregate, normalize
from pycytominer.cyto_utils import (
    get_default_compartments,
    get_default_linking_cols,
    infer_cp_features,
)
from pycytominer.cyto_utils.cells import SingleCells, _sqlite_strata_conditions

# reference https://github.com/cytomining/pycytominer/issues/195
# shrunk file for quicker testing as per work within shrink-demo-file.ipynb
sql_path = "testing_SQ00014613.sqlite"
sql_url = "sqlite:///testing_SQ00014613.sqlite"

# referenced from https://github.com/cytomining/pycytominer/blob/master/pycytominer/cyto_utils/cells.py
# a new singlecell class to manage modifications for testing
class new_SingleCells(SingleCells):
    def load_image(self):
        """Load image table from sqlite file

        Returns
        -------
        None
            Nothing is returned.
        """

        image_query = "select * from image"
        self.image_df = pd.read_sql(sql=image_query, con=sql_url)

        if self.add_image_features:
            self.image_features_df = extract_image_features(
                self.image_feature_categories,
                self.image_df,
                self.image_cols,
                self.strata,
            )

        image_features = list(np.union1d(self.image_cols, self.strata))
        self.image_df = self.image_df[image_features]

        if self.fields_of_view != "all":
            check_fields_of_view(
                list(np.unique(self.image_df[self.fields_of_view_feature])),
                list(self.fields_of_view),
            )
            self.image_df = self.image_df.query(
                f"{self.fields_of_view_feature}==@self.fields_of_view"
            )

            if self.add_image_features:
                self.image_features_df = self.image_features_df.query(
                    f"{self.fields_of_view_feature}==@self.fields_of_view"
                )

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
                    initial_df = initial_df.set_index(
                        self.merge_cols + [left_link_col], drop=False
                    )
                    if compute_subsample:
                        # Sample cells proportionally by self.strata
                        self.get_subsample(df=initial_df, rename_col=False)

                        subset_logic_df = self.subset_data_df.drop(
                            self.image_df.columns, axis="columns"
                        )

                        initial_df = subset_logic_df.merge(
                            initial_df, how="left", on=subset_logic_df.columns.tolist()
                        ).reindex(initial_df.columns, axis="columns")

                    print(self.merge_cols + [left_link_col])
                    print(self.merge_cols + [right_link_col])
                    sc_df = initial_df.merge(
                        self.load_compartment(compartment=right_compartment).set_index(
                            self.merge_cols + [right_link_col], drop=False
                        ),
                        how="inner",
                        left_index=True,
                        right_index=True,
                        suffixes=merge_suffix,
                    )
                else:
                    sc_df = sc_df.merge(
                        self.load_compartment(compartment=right_compartment).set_index(
                            self.merge_cols + [right_link_col], drop=False
                        ),
                        left_index=True,
                        right_index=True,
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
        sc_df = sc_df.set_index(self.merge_cols, drop=False)
        self.image_df = self.image_df.set_index(self.merge_cols, drop=False)
        sc_df = (
            sc_df.merge(self.image_df, how="left", left_index=True, right_index=True)
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

    def load_compartment(self, compartment):
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
        print(compartment)
        compartment_query = "select * from {}".format(compartment)  # nosec
        df = pd.read_sql(compartment_query, con=sql_url)
        return df


def mem_profile_func():
    """
    wrapper function for memory profiling
    """

    sc_p = new_SingleCells(
        sql_url,
        strata=["Image_Metadata_Plate", "Image_Metadata_Well"],
        image_cols=["TableNumber", "ImageNumber"],
        fields_of_view_feature=[],
    )
    return sc_p.merge_single_cells()


print(mem_profile_func().info())

import types

import pandas as pd
from pycytominer import normalize
from pycytominer.cyto_utils import (
    infer_cp_features,
)
from pycytominer.cyto_utils.cells import SingleCells

# reference https://github.com/cytomining/pycytominer/issues/195
# shrunk file for quicker testing as per work within shrink-demo-file.ipynb
sql_path = "testing_SQ00014613.sqlite"
sql_url = "sqlite:///testing_SQ00014613.sqlite"

# referenced from https://github.com/cytomining/pycytominer/blob/master/pycytominer/cyto_utils/cells.py
# a new singlecell class to manage modifications for testing
class new_SingleCells(SingleCells):

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
        print(self.compartment_linking_cols)
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

        # combine rename dictionaries to reduce resource load
        merge_rename = self.full_merge_suffix_rename.copy()
        for key, val in self.linking_col_rename.items():
            if val in merge_rename.keys():
                # if the val exists as a key in full_merge_suffix_rename
                # set a key with the value
                merge_rename[key] = merge_rename[val]
                # remove the key with the previous value name
                merge_rename.pop(val)

        print(self.image_df.info())
        print(sc_df.info())
        sc_df = sc_df.merge(self.image_df, on=self.merge_cols, how="left")
        sc_df = sc_df.rename(merge_rename, axis="columns", copy=False, inplace=False)
        print(sc_df.info())

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
        compartment_query = "select * from {}".format(compartment)  # nosec
        df = pd.read_sql(sql=compartment_query, con=self.conn)
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
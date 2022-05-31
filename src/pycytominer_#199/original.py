# reference https://github.com/cytomining/pycytominer/issues/195
from pycytominer.cyto_utils.cells import SingleCells

sql_path = "testing_SQ00014613.sqlite"
sql_url = f"sqlite:///{sql_path}"

sc_p = SingleCells(
    sql_url,
    strata=["Image_Metadata_Plate", "Image_Metadata_Well"],
    image_cols=["TableNumber", "ImageNumber"],
    fields_of_view_feature=[],
)

merged_sc = sc_p.merge_single_cells()

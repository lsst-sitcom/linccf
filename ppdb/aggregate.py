import hats as hc
import numpy as np
import pandas as pd
from lsdb import Catalog
from lsdb.dask.merge_catalog_functions import (
    align_and_apply,
    align_catalogs,
    construct_catalog_args,
    get_healpix_pixels_from_alignment,
    filter_by_spatial_index_to_pixel,
)


def aggregate_object_data(dia_object_lc):
    alignment = align_catalogs(dia_object_lc, dia_object_lc)
    _, pixels = get_healpix_pixels_from_alignment(alignment)
    joined_partitions = align_and_apply(
        [(dia_object_lc, pixels), (dia_object_lc.margin, pixels)],
        perform_join_on,
    )
    ddf, ddf_map, alignment = construct_catalog_args(
        joined_partitions,
        dia_object_lc._ddf._meta,
        alignment,
    )
    hc_catalog = hc.catalog.Catalog(
        dia_object_lc.hc_structure.catalog_info,
        alignment.pixel_tree,
        schema=dia_object_lc.original_schema,  # the schema is the same
        moc=alignment.moc,
    )
    return Catalog(ddf, ddf_map, hc_catalog)


def perform_join_on(df, margin, df_pixel, *args):
    original_cols = list(df.columns)

    # 1. Join df with margin
    final_df = pd.concat([df, margin])

    # 2. Order each object by validityStart
    final_df = final_df.sort_values(["diaObjectId", "validityStart"], ascending=[True,False])

    # 3. Get the sources for all the objects
    final_df["diaSource.diaObjectId"] = final_df["diaObjectId"]
    final_df["diaForcedSource.diaObjectId"] = final_df["diaObjectId"]
    sources = final_df["diaSource"].explode().sort_values(["midpointMjdTai"])
    fsources = final_df["diaForcedSource"].explode().sort_values(["midpointMjdTai"])

    # 4. Grab the latest row per object
    _, latest_indices = np.unique(final_df["diaObjectId"], return_index=True)
    final_df = final_df.iloc[latest_indices]

    # 5. Drop the sources and join them again
    final_df = final_df.drop(columns=["diaSource", "diaForcedSource"])
    final_df = final_df.join_nested(sources, "diaSource", on="diaObjectId")
    final_df = final_df.join_nested(fsources, "diaForcedSource", on="diaObjectId")

    # 6. Filter out points outside of the pixel (that are therefore in margin)
    final_df = filter_by_spatial_index_to_pixel(final_df, df_pixel.order, df_pixel.pixel)

    # 7. Make sure columns keep the same order
    return final_df[original_cols]

"""Methods for post-processing PPDB data"""

from datetime import datetime, timezone

import astropy.units as u
import hats
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dask.distributed import as_completed
from hats.io.parquet_metadata import write_parquet_metadata
from tqdm import tqdm


def postprocess_catalog(catalog_name, flux_col_prefixes, hats_dir, client):
    catalog_dir = f"{hats_dir}/{catalog_name}"
    catalog = hats.read_hats(catalog_dir)
    futures = []
    for target_pixel in catalog.get_healpix_pixels():
        futures.append(
            client.submit(
                process_partition,
                catalog_dir=catalog_dir,
                target_pixel=target_pixel,
                flux_col_prefixes=flux_col_prefixes,
            )
        )
    for future in tqdm(as_completed(futures), desc=catalog_name, total=len(futures)):
        if future.status == "error":
            raise future.exception()
    rewrite_catalog_metadata(catalog, hats_dir)


def process_partition(catalog_dir, target_pixel, flux_col_prefixes):
    """Apply post-processing steps to each individual partition"""
    # Read partition
    file_path = hats.io.pixel_catalog_file(catalog_dir, target_pixel)
    table = pd.read_parquet(file_path, dtype_backend="pyarrow")
    # Apply transformations
    if "validityStart" in table.columns:
        table = select_by_latest_validity(table)
    if len(flux_col_prefixes) > 0:
        table = append_mag_and_magerr(table, flux_col_prefixes)
    table = cast_columns_float32(table)
    # Overwrite partition
    final_table = pa.Table.from_pandas(table, preserve_index=False)
    pq.write_table(final_table.replace_schema_metadata(), file_path.path)


def rewrite_catalog_metadata(catalog, hats_dir):
    """Update catalog metadata after processing the leaf parquet files"""
    destination_path = f"{hats_dir}/{catalog.catalog_name}"
    # Update _common_metadata and _metadata
    parquet_rows = write_parquet_metadata(destination_path)
    # Update hats.properties
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M%Z")
    updated_info = catalog.catalog_info.copy_and_update(
        total_rows=parquet_rows, hats_creation_date=now
    )
    updated_info.to_properties_file(destination_path)


def select_by_latest_validity(table):
    """Select rows with the latest validityStart for each object."""
    return table.sort_values("validityStart").drop_duplicates(
        "diaObjectId", keep="last"
    )


def append_mag_and_magerr(table, flux_cols):
    """Calculate magnitudes and their errors for flux columns."""
    mag_cols = {}
    for flux_col in flux_cols:
        flux_col_err = f"{flux_col}Err"
        mag_col = flux_col.replace("Flux", "Mag")
        mag_col_err = f"{mag_col}Err"
        # Set magnitude column
        flux = table[flux_col]
        mag = u.nJy.to(u.ABmag, flux)
        mag_cols[mag_col] = mag
        # Set magnitude error column
        flux_err = table[flux_col_err]
        upper_mag = u.nJy.to(u.ABmag, flux + flux_err)
        lower_mag = u.nJy.to(u.ABmag, flux - flux_err)
        magErr = -(upper_mag - lower_mag) / 2
        mag_cols[mag_col_err] = magErr
    mag_table = pd.DataFrame(
        mag_cols, dtype=pd.ArrowDtype(pa.float32()), index=table.index
    )
    return pd.concat([table, mag_table], axis=1)


def cast_columns_float32(table):
    """Cast non-(positional/time) columns to single-precision"""
    position_time_cols = [
        "ra",
        "dec",
        "raErr",
        "decErr",
        "x",
        "y",
        "xErr",
        "yErr",
        "midpointMjdTai",
        "radecMjdTai",
    ]
    columns_to_cast = [
        field
        for (field, type) in table.dtypes.items()
        if field not in position_time_cols and type == pd.ArrowDtype(pa.float64())
    ]
    dtype_map = {col: pd.ArrowDtype(pa.float32()) for col in columns_to_cast}
    return table.astype(dtype_map)

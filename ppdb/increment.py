"""Utilities to write increment data and metadata"""

from datetime import datetime

import dask
import hats
import numpy as np
from hats.catalog import PartitionInfo
from hats.io import paths
from hats.io.file_io import write_fits_image
from hats.io.paths import PARTITION_PIXEL
from hats.io.skymap import read_skymap, write_skymap
from hats.pixel_math.sparse_histogram import HistogramAggregator, SparseHistogram
from lsdb.catalog.dataset.dataset import Dataset
from lsdb.io.to_hats import calculate_histogram, create_modified_catalog_structure

# The new parquet files will be named after the current date.
NPIX_SUFFIX = f"/{datetime.now().strftime('%Y-%m-%d')}.parquet"


def write_partitions(catalog, base_catalog_dir, histogram_order, **kwargs):
    """Saves catalog partitions as parquet to disk"""
    results, pixels = [], []
    partitions = catalog._ddf.to_delayed()

    for pixel, partition_index in catalog._ddf_pixel_map.items():
        results.append(
            perform_write(
                partitions[partition_index],
                pixel,
                base_catalog_dir,
                histogram_order,
                **kwargs,
            )
        )
        pixels.append(pixel)

    if len(results) > 0:
        results = dask.compute(*results)
        counts, histograms = list(zip(*results))
    else:
        counts, histograms = (), ()

    non_empty_indices = np.nonzero(counts)
    non_empty_pixels = np.array(pixels)[non_empty_indices]
    non_empty_counts = np.array(counts)[non_empty_indices]
    non_empty_hists = np.array(histograms)[non_empty_indices]
    return list(non_empty_pixels), list(non_empty_counts), list(non_empty_hists)


@dask.delayed
def perform_write(df, hp_pixel, base_catalog_dir, histogram_order, **kwargs):
    if len(df) == 0:
        return 0, SparseHistogram([], [], histogram_order)
    # The parquet leaf files live in a pixel directory. Create it if it does not exist.
    pixel_dir = (
        hats.io.pixel_directory(base_catalog_dir, hp_pixel.order, hp_pixel.pixel)
        / f"{PARTITION_PIXEL}={hp_pixel.pixel}"
    )
    hats.io.file_io.make_directory(pixel_dir, exist_ok=True)
    pixel_path = paths.pixel_catalog_file(
        base_catalog_dir, hp_pixel, npix_suffix=NPIX_SUFFIX
    )
    df.to_parquet(pixel_path.path, filesystem=pixel_path.fs, **kwargs)
    return len(df), calculate_histogram(df, histogram_order)


def update_skymaps(existing_catalog, histograms, histogram_order):
    catalog_base_dir = existing_catalog.hc_structure.catalog_path
    existing_histogram = read_skymap(existing_catalog.hc_structure, histogram_order)

    total_histogram = HistogramAggregator(histogram_order)
    for partition_hist in histograms:
        total_histogram.add(partition_hist)

    # Also add the existing histogram
    full_histogram = total_histogram.full_histogram + existing_histogram

    # Write skymaps to point_map.fits and skymap_*.fits
    map_file_path = paths.get_point_map_file_pointer(catalog_base_dir)
    write_fits_image(full_histogram, map_file_pointer=map_file_path)
    skymap_alt_orders = existing_catalog.hc_structure.catalog_info.skymap_alt_orders
    write_skymap(full_histogram, catalog_dir=catalog_base_dir, orders=skymap_alt_orders)


def update_metadata(existing_catalog, new_pixels, new_counts):
    catalog_base_dir = existing_catalog.hc_structure.catalog_path

    # Delete _metadata, since its statistics are outdated
    paths.get_parquet_metadata_pointer(catalog_base_dir).unlink(missing_ok=True)

    # Update `partition_info.csv` with new set of pixels
    pixels = set(existing_catalog.get_healpix_pixels())
    pixels.update(new_pixels)
    partition_info = PartitionInfo.from_healpix(list(pixels))
    partition_info_file = paths.get_partition_info_pointer(catalog_base_dir)
    partition_info.write_to_file(partition_info_file)

    # Update the `hats.properties`
    old_properties = existing_catalog.hc_structure.catalog_info
    previous_total_rows = int(old_properties.total_rows)
    total_rows = previous_total_rows + int(np.sum(new_counts))
    previous_max_rows = int(old_properties.hats_max_rows)
    max_rows = max(previous_max_rows, max(new_counts))

    new_hc_structure = create_modified_catalog_structure(
        existing_catalog.hc_structure,
        catalog_base_dir,
        existing_catalog.hc_structure.catalog_name,
        total_rows=total_rows,
        hats_max_rows=max_rows,
        hats_order=partition_info.get_highest_order(),
        moc_sky_fraction=f"{partition_info.calculate_fractional_coverage():0.5f}",
        **Dataset.new_provenance_properties(catalog_base_dir),
    )
    new_hc_structure.catalog_info.to_properties_file(catalog_base_dir)

"""Pipeline to import catalog with existing partitioning"""

import os

import cloudpickle
import hats.io.file_io as file_io
import hats.io.paths as paths
import hats_import.catalog.map_reduce as mr
import numpy as np
from hats.catalog import PartitionInfo
from hats.io.parquet_metadata import write_parquet_metadata
from hats.io.skymap import write_skymap
from hats.io.validation import is_valid_catalog
from hats.loaders.read_hats import _read_schema_from_metadata
from hats.pixel_math.healpix_pixel import HealpixPixel


def map_pixels(args, resume_plan, client):
    """Creates raw histogram for the new data"""
    # Pickle a file reader to be used throughout the pipeline
    pickled_reader_file = os.path.join(resume_plan.tmp_path, "reader.pickle")
    with open(pickled_reader_file, "wb") as pickle_file:
        cloudpickle.dump(args.file_reader, pickle_file)

    futures = []
    for key, file_path in resume_plan.map_files:
        futures.append(
            client.submit(
                mr.map_to_pixels,
                input_file=file_path,
                resume_path=resume_plan.tmp_path,
                pickled_reader_file=pickled_reader_file,
                mapping_key=key,
                highest_order=args.mapping_healpix_order,
                ra_column=args.ra_column,
                dec_column=args.dec_column,
                use_healpix_29=args.use_healpix_29,
            )
        )
    resume_plan.wait_for_mapping(futures)

    # Load histogram and total counts
    raw_histogram = resume_plan.read_histogram(args.mapping_healpix_order)
    total_rows = int(raw_histogram.sum())
    return raw_histogram, total_rows, pickled_reader_file


def binning(args, resume_plan, raw_histogram, existing_pixels, expected_total_rows):
    """Creates the alignment considering an existing partitioning"""

    # Keep all existing pixels as is and add points outside of that coverage
    # to new pixels. These should be the largest non-overlapping pixels possible.
    alignment = _find_largest_nonoverlapping_pixel(
        raw_histogram, args.mapping_healpix_order, existing_pixels
    )

    # Write alignment to file
    alignment_file = file_io.append_paths_to_pointer(
        resume_plan.tmp_path, resume_plan.ALIGNMENT_FILE
    )
    with open(alignment_file, "wb") as pickle_file:
        alignment = np.array(
            [x if x is not None else [-1, -1, -1] for x in alignment], dtype=np.int64
        )
        cloudpickle.dump(alignment, pickle_file)

    # Create destination pixel map
    pixel_list = np.unique(alignment, axis=0)
    destination_pixel_map = {
        HealpixPixel(order, pix): count
        for (order, pix, count) in pixel_list
        if int(count) > 0
    }
    total_rows = sum(destination_pixel_map.values())
    if total_rows != expected_total_rows:
        raise ValueError(
            f"Number of rows ({total_rows}) does not match expectation ({expected_total_rows})"
        )
    resume_plan.destination_pixel_map = destination_pixel_map
    return alignment_file


def _find_largest_nonoverlapping_pixel(
    histogram,
    mapping_order,
    existing_pixels,
    lowest_order=0,
    threshold=np.inf,
):
    n_leaf = len(histogram)
    orig = histogram.copy()
    assignment = np.full((n_leaf, 2), -1, dtype=int)

    # Mark leaves for existing partitions.
    blocked = np.zeros(n_leaf, dtype=bool)
    for order, pix in existing_pixels:
        start, end = _descendants_of(order, pix, mapping_order)
        assignment[start:end, 0] = order
        assignment[start:end, 1] = pix
        blocked[start:end] = True

    # Active leaves: have data and are not in existing pixels.
    active_mask = (orig > 0) & (~blocked)
    assignment[active_mask, 0] = mapping_order
    assignment[active_mask, 1] = np.nonzero(active_mask)[0]

    h = orig.copy()
    h[blocked] = 0
    order = mapping_order

    while order > lowest_order:
        n_parent = len(h) // 4
        h4 = h.reshape(n_parent, 4)
        b4 = blocked.reshape(n_parent, 4)

        parent_counts = h4.sum(axis=1)
        blocked_parent = b4.any(axis=1)

        mergeable = (
            (~blocked_parent) & (parent_counts > 0) & (parent_counts <= threshold)
        )
        if mergeable.any():
            parent_ids = np.nonzero(mergeable)[0]
            children = (parent_ids[:, None] * 4) + np.arange(4)
            children_flat = children.ravel()
            has_data = orig[children_flat] > 0
            if has_data.any():
                idx_to_update = children_flat[has_data]
                parent_for_child = np.repeat(parent_ids, 4)[has_data]
                assignment[idx_to_update, 0] = order - 1
                assignment[idx_to_update, 1] = parent_for_child

        h = parent_counts
        blocked = blocked_parent
        order -= 1

    alignment = np.empty((n_leaf, 3), dtype=int)
    alignment[:, 0:2] = assignment
    alignment[:, 2] = orig
    return _propagate_parent_counts(alignment)


def _descendants_of(order, pix, mapping_order):
    """Return the (order, pix) children pixels at mapping_order"""
    factor = 4 ** (mapping_order - order)
    start = pix * factor
    end = (pix + 1) * factor
    return start, end


def _propagate_parent_counts(alignment):
    """Propagate the parent counts to their respective leaves"""
    result = alignment.copy()
    valid_mask = result[:, 0] != -1
    if not valid_mask.any():
        return result
    parent_keys = result[valid_mask, 0] * 10**8 + result[valid_mask, 1]
    unique_keys, inverse = np.unique(parent_keys, return_inverse=True)
    summed_counts = np.zeros(len(unique_keys), dtype=result.dtype)
    np.add.at(summed_counts, inverse, result[valid_mask, 2])
    result[valid_mask, 2] = summed_counts[inverse]
    return result


def split_pixels(args, resume_plan, alignment_file, pickled_reader_file, client):
    """Split input files into final pixel shards"""
    futures = []
    for key, file_path in resume_plan.split_keys:
        futures.append(
            client.submit(
                mr.split_pixels,
                input_file=file_path,
                pickled_reader_file=pickled_reader_file,
                highest_order=args.mapping_healpix_order,
                ra_column=args.ra_column,
                dec_column=args.dec_column,
                splitting_key=key,
                cache_shard_path=args.tmp_path,
                resume_path=resume_plan.tmp_path,
                alignment_file=alignment_file,
                use_healpix_29=args.use_healpix_29,
            )
        )
    resume_plan.wait_for_splitting(futures)


def reduce_pixels(args, resume_plan, client):
    """Reduce pixel shards for the final pixels"""
    futures = []
    for (
        destination_pixel,
        source_pixel_count,
        destination_pixel_key,
    ) in resume_plan.get_reduce_items():
        futures.append(
            client.submit(
                mr.reduce_pixel_shards,
                cache_shard_path=args.tmp_path,
                resume_path=resume_plan.tmp_path,
                reducing_key=destination_pixel_key,
                destination_pixel_order=destination_pixel.order,
                destination_pixel_number=destination_pixel.pixel,
                destination_pixel_size=source_pixel_count,
                output_path=args.catalog_path,
                ra_column=args.ra_column,
                dec_column=args.dec_column,
                sort_columns=args.sort_columns,
                add_healpix_29=args.add_healpix_29,
                use_schema_file=args.use_schema_file,
                use_healpix_29=args.use_healpix_29,
                delete_input_files=args.delete_intermediate_parquet_files,
                write_table_kwargs=args.write_table_kwargs,
                row_group_kwargs=args.row_group_kwargs,
                npix_suffix=args.npix_suffix,
            )
        )
    resume_plan.wait_for_reducing(futures)


def finalize(args, resume_plan, raw_histogram, total_rows):
    """Create metadata files, properties and skymaps"""
    with resume_plan.print_progress(total=6, stage_name="Finishing") as step_progress:
        partition_info = PartitionInfo.from_healpix(
            resume_plan.get_destination_pixels()
        )
        partition_info_file = paths.get_partition_info_pointer(args.catalog_path)
        partition_info.write_to_file(partition_info_file)
        step_progress.update(1)
        column_names = []
        if not args.debug_stats_only:
            parquet_rows = write_parquet_metadata(
                args.catalog_path,
                create_thumbnail=args.create_thumbnail,
                thumbnail_threshold=args.pixel_threshold,
            )
            if total_rows > 0 and parquet_rows != total_rows:
                raise ValueError(
                    f"Number of rows in parquet ({parquet_rows}) "
                    f"does not match expectation ({total_rows})"
                )
            column_names = _read_schema_from_metadata(args.catalog_path).names
        step_progress.update(1)
        file_io.write_fits_image(
            raw_histogram, paths.get_point_map_file_pointer(args.catalog_path)
        )
        write_skymap(
            histogram=raw_histogram,
            catalog_dir=args.catalog_path,
            orders=args.skymap_alt_orders,
        )
        step_progress.update(1)
        catalog_info = args.to_table_properties(
            total_rows,
            partition_info.get_highest_order(),
            partition_info.calculate_fractional_coverage(),
            column_names=column_names,
        )
        catalog_info.to_properties_file(args.catalog_path)
        step_progress.update(1)
        resume_plan.clean_resume_files()
        step_progress.update(1)
        assert is_valid_catalog(args.catalog_path)
        step_progress.update(1)

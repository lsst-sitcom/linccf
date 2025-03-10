from hats_import.catalog.arguments import ImportArguments
import hats_import.pipeline as runner
import os

data_dir = "/sdf/data/rubin/u/olynn/sdss/"

if __name__ == '__main__':
    args = ImportArguments(
        output_artifact_name="sdss",
        input_path=data_dir,
        file_reader="fits",
        ra_column="RA",
        dec_column="DEC",
        sort_columns="SDSS_NAME",
        pixel_threshold=1_000_000,
        highest_healpix_order=7,
        output_path=data_dir,
    )
    runner.pipeline(args)
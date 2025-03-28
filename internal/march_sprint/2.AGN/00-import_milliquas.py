import tempfile

from dask.distributed import Client
from hats_import.catalog.arguments import ImportArguments
from hats_import.margin_cache.margin_cache_arguments import MarginCacheArguments
import hats_import.pipeline as runner

data_in = "/sdf/data/rubin/u/olynn/AGNs/fits"
data_out = "/sdf/data/rubin/u/olynn/AGNs/hats"


def main_pipeline():
    tmp_path = tempfile.TemporaryDirectory(dir="/sdf/data/rubin/u/olynn/AGNs/tmp")
    tmp_dir = tmp_path.name
    with Client(n_workers=8, threads_per_worker=1, local_directory=tmp_dir) as client:
        args = ImportArguments(
            output_artifact_name="Milliquas_v8",
            input_path=data_in,
            file_reader="fits",
            ra_column="RA",
            dec_column="DEC",
            sort_columns="NAME",
            pixel_threshold=1_000_000,
            highest_healpix_order=7,
            output_path=data_out,
        )
        runner.pipeline(args)


cat_in = "/sdf/data/rubin/u/olynn/AGNs/hats/Milliquas_v8"
margin_out = "/sdf/data/rubin/u/olynn/AGNs/hats/Milliquas_v8_margin"


def margin_pipeline():
    tmp_path = tempfile.TemporaryDirectory(dir="/sdf/data/rubin/u/olynn/AGNs/tmp")
    tmp_dir = tmp_path.name
    with Client(n_workers=8, threads_per_worker=1, local_directory=tmp_dir) as client:
        args = MarginCacheArguments(
            input_catalog_path=cat_in,
            output_path=margin_out,
            margin_threshold=10.0,
            output_artifact_name="Milliquas_v8_10arcs",
        )
        runner.pipeline(args)


if __name__ == "__main__":
    margin_pipeline()

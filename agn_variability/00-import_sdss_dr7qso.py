from hats_import.catalog.arguments import ImportArguments
import tempfile
import hats_import.pipeline as runner
from dask.distributed import Client

data_in = "/data3/epyc/data3/hats/raw/sdss_in/"
data_out = "/data3/epyc/data3/hats/raw/sdss/sdss_dr7_qso_props/"

if __name__ == '__main__':
    tmp_path = tempfile.TemporaryDirectory(dir="/data3/epyc/data3/hats/raw/")
    tmp_dir = tmp_path.name
    with Client(n_workers=8, threads_per_worker=1, local_directory=tmp_dir) as client:
        args = ImportArguments(
            output_artifact_name="sdss_dr7_qso_props",
            input_path=data_in,
            file_reader="fits",
            ra_column="RA",
            dec_column="DEC",
            sort_columns="SDSS_NAME",
            pixel_threshold=1_000_000,
            highest_healpix_order=7,
            output_path=data_out,
        )
        runner.pipeline(args)

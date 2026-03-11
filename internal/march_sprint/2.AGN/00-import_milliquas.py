import tempfile
from pathlib import Path
import zipfile

from dask.distributed import Client
from hats_import.catalog.arguments import ImportArguments
from hats_import.margin_cache.margin_cache_arguments import MarginCacheArguments
import hats_import.pipeline as runner

data_in = Path("/sdf/data/rubin/u/olynn/AGNs_tmp/raw_data") # Where you downloaded the zip file
data_out = Path("/sdf/data/rubin/u/olynn/AGNs_tmp/hats") # Where you want your hats catalog to go


def main_pipeline():
    tmp_path = tempfile.TemporaryDirectory(dir=data_out)
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


cat_in = data_out / "Milliquas_v8"
margin_out = data_out / "Milliquas_v8_margin"


def margin_pipeline():
    tmp_path = tempfile.TemporaryDirectory(dir=data_out)
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
    # Ensure data_out directory exists
    data_out.mkdir(parents=True, exist_ok=True)

    # Unzip milliquas.fits.zip if milliquas.fits doesn’t exist
    zip_file = data_in / "milliquas.fits.zip"
    fits_file = data_in / "milliquas.fits"
    if not fits_file.exists():
        print(f"Unzipping {zip_file}...")
        with zipfile.ZipFile(zip_file, "r") as zip_ref:
            zip_ref.extractall(data_in)
        print("Unzip complete. Deleting zip file...")
        zip_file.unlink()  # Delete the zip file
        print(f"{zip_file} deleted.")
    else:
        print(f"{fits_file} already exists. Skipping unzip.")

    # Run the pipeline
    main_pipeline()
    margin_pipeline()
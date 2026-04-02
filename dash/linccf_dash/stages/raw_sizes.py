from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from lsst.resources import ResourcePath
from tqdm import tqdm

from linccf_dash.config import CatalogConfig, PipelineConfig


def run_raw_sizes(cfg: PipelineConfig, catalog_filter: Optional[list[str]] = None) -> None:
    raw_dir = cfg.run.raw_dir
    index_dir = raw_dir / "index"
    index_dir.mkdir(parents=True, exist_ok=True)

    for catalog_name, catalog_cfg in cfg.enabled_catalogs(catalog_filter).items():
        _build_sizes_csv(catalog_name, catalog_cfg, raw_dir)
        _write_index_files(catalog_name, catalog_cfg, raw_dir, index_dir)


def _build_sizes_csv(catalog_name: str, catalog_cfg: CatalogConfig, raw_dir: Path) -> None:
    """Join refs CSV with paths and sample 100 files to estimate pixel thresholds."""
    paths_file = raw_dir / "paths" / f"{catalog_name}.txt"
    paths = [p.strip() for p in paths_file.read_text(encoding="utf8").splitlines() if p.strip()]
    print(f"Found {len(paths)} files for {catalog_name}")

    ref_frame = pd.read_csv(raw_dir / "refs" / f"{catalog_name}.csv")
    ref_frame["path"] = paths
    ref_frame.to_csv(raw_dir / "sizes" / f"{catalog_name}.csv", index=False)

    # Sample up to 100 files to estimate partition thresholds
    num_rows_sample: list[int] = []
    file_sizes_sample: list[int] = []

    for path_str in tqdm(paths[:100], desc=catalog_name):
        rp = ResourcePath(path_str)
        with rp.open("rb") as f:
            md = pq.ParquetFile(f).metadata
            num_rows_sample.append(md.num_rows)
        file_sizes_sample.append(rp.size())

    total_size = np.array(file_sizes_sample).sum()
    total_rows = np.array(num_rows_sample).sum()
    mb300 = 300 * 1024 * 1024
    gb1 = 1024 * 1024 * 1024
    lo = int(mb300 / total_size * total_rows)
    hi = int(gb1 / total_size * total_rows)
    print(f"  {catalog_name}: estimated pixel_threshold between {lo:_} and {hi:_}")


def _write_index_files(
    catalog_name: str, catalog_cfg: CatalogConfig, raw_dir: Path, index_dir: Path
) -> None:
    """Group the sizes CSV by group_by columns and write one index CSV per group."""
    ref_frame = pd.read_csv(raw_dir / "sizes" / f"{catalog_name}.csv")
    desired_columns = catalog_cfg.dims + ["path"]
    ref_frame = ref_frame[desired_columns]

    out_dir = index_dir / catalog_name
    out_dir.mkdir(parents=True, exist_ok=True)

    groups = ref_frame.groupby(catalog_cfg.group_by)
    for counter, (_, group) in enumerate(groups):
        group.to_csv(out_dir / f"{counter:03d}.csv", index=False)

    print(f"Wrote {counter + 1} index files for {catalog_name}")

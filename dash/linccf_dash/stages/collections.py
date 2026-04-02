from __future__ import annotations

import shutil

from hats_import import pipeline_with_client
from hats_import.collection.arguments import CollectionArguments

from linccf_dash.config import PipelineConfig
from linccf_dash.utils.dask_client import dask_client


def run_collections(cfg: PipelineConfig) -> None:
    hats_dir = cfg.run.hats_dir

    with dask_client(n_workers=16, threads_per_worker=1) as client:
        for collection_name, collection_cfg in cfg.collections.items():
            nested_name = collection_cfg.nested_catalog
            collection_dir = hats_dir / collection_name
            nested_dest = collection_dir / nested_name

            # Move nested catalog into the collection directory
            collection_dir.mkdir(exist_ok=True)
            shutil.move(str(hats_dir / nested_name), str(nested_dest))

            args = (
                CollectionArguments(
                    output_artifact_name=collection_name,
                    new_catalog_name=nested_name,
                    output_path=hats_dir,
                    simple_progress_bar=True,
                )
                .catalog(catalog_path=nested_dest)
                .add_margin(margin_threshold=collection_cfg.margin_threshold, is_default=True)
                .add_index(indexing_column=collection_cfg.index_column)
            )
            pipeline_with_client(args, client)

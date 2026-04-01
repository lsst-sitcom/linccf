from __future__ import annotations

import tempfile
from contextlib import contextmanager
from typing import Optional

from dask.distributed import Client


@contextmanager
def dask_client(
    n_workers: int = 8,
    threads_per_worker: int = 1,
    memory_limit: Optional[str] = None,
):
    """Context manager that creates a Dask client with a temporary local directory."""
    tmp = tempfile.TemporaryDirectory()
    kwargs: dict = {
        "n_workers": n_workers,
        "threads_per_worker": threads_per_worker,
        "local_directory": tmp.name,
    }
    if memory_limit is not None:
        kwargs["memory_limit"] = memory_limit
    client = Client(**kwargs)
    try:
        yield client
    finally:
        client.close()
        tmp.cleanup()

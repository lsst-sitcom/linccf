from __future__ import annotations

import os
import socket
import subprocess
import time
from pathlib import Path
from typing import Optional

import typer

from linccf_dash.config import PipelineConfig, load_config
from linccf_dash.stages.butler import run_butler
from linccf_dash.stages.collections import run_collections
from linccf_dash.stages.crossmatch import run_crossmatch
from linccf_dash.stages.generate_json import run_generate_json
from linccf_dash.stages.import_catalogs import run_import
from linccf_dash.stages.nesting import run_nesting
from linccf_dash.stages.postprocess import run_postprocess
from linccf_dash.stages.raw_sizes import run_raw_sizes

app = typer.Typer(help="DASH Import Pipeline — convert Rubin DRP outputs to HATS catalogs.")

STAGE_ORDER = [
    "butler",
    "raw_sizes",
    "import",
    "postprocess",
    "nesting",
    "collections",
    "crossmatch",
    "generate_json",
]

# Stages that require the LSST stack to be active
LSST_STAGES = {"butler", "raw_sizes", "import"}


def _check_lsst() -> None:
    try:
        import lsst.resources  # noqa: F401
    except ImportError:
        typer.echo(
            "Error: LSST stack is not available.\n"
            "Please activate it before running:\n"
            "  source /sdf/group/rubin/sw/loadLSST.sh && setup lsst_distrib",
            err=True,
        )
        raise typer.Exit(1)


def _resolve_stages(
    cfg: PipelineConfig,
    stages_opt: Optional[str],
    from_stage_opt: Optional[str],
) -> list[str]:
    if stages_opt and from_stage_opt:
        typer.echo("Error: --stages and --from-stage are mutually exclusive.", err=True)
        raise typer.Exit(1)

    if stages_opt:
        requested = [s.strip() for s in stages_opt.split(",")]
    elif from_stage_opt:
        if from_stage_opt not in STAGE_ORDER:
            typer.echo(f"Error: unknown stage '{from_stage_opt}'. Valid stages: {', '.join(STAGE_ORDER)}", err=True)
            raise typer.Exit(1)
        start = STAGE_ORDER.index(from_stage_opt)
        requested = [s for s in STAGE_ORDER[start:] if s in cfg.stages.enabled]
    else:
        requested = list(cfg.stages.enabled)

    invalid = set(requested) - set(STAGE_ORDER)
    if invalid:
        typer.echo(f"Error: unknown stage(s): {', '.join(sorted(invalid))}. Valid stages: {', '.join(STAGE_ORDER)}", err=True)
        raise typer.Exit(1)

    # Return in pipeline order
    return [s for s in STAGE_ORDER if s in requested]


def _run_stage(stage: str, cfg: PipelineConfig, catalog_filter: Optional[list[str]]) -> None:
    if stage == "butler":
        run_butler(cfg, catalog_filter)
    elif stage == "raw_sizes":
        run_raw_sizes(cfg, catalog_filter)
    elif stage == "import":
        run_import(cfg, catalog_filter)
    elif stage == "postprocess":
        run_postprocess(cfg, catalog_filter)
    elif stage == "nesting":
        run_nesting(cfg)
    elif stage == "collections":
        run_collections(cfg)
    elif stage == "crossmatch":
        run_crossmatch(cfg)
    elif stage == "generate_json":
        run_generate_json(cfg)


@app.command()
def run(
    config_paths: list[Path] = typer.Option(..., "--config", "-c", help="TOML config file(s). Specify multiple times to layer overrides on top of each other (left to right)."),
    stages: Optional[str] = typer.Option(
        None, "--stages", help="Comma-separated list of stages to run (e.g. butler,import,postprocess)."
    ),
    from_stage: Optional[str] = typer.Option(
        None, "--from-stage", help="Run all enabled stages starting from this one."
    ),
    catalogs: Optional[str] = typer.Option(
        None, "--catalogs", help="Comma-separated catalog names to process (e.g. dia_object,object)."
    ),
) -> None:
    """Run the DASH pipeline."""
    cfg = load_config(config_paths)
    stages_to_run = _resolve_stages(cfg, stages, from_stage)

    if any(s in LSST_STAGES for s in stages_to_run):
        _check_lsst()

    catalog_filter = [c.strip() for c in catalogs.split(",")] if catalogs else None

    active_catalogs = list(cfg.enabled_catalogs(catalog_filter).keys())

    typer.echo(f"----- DASH Import Pipeline -----")
    typer.echo(f"Version : {cfg.run.version}")
    typer.echo(f"Stages  : {', '.join(stages_to_run)}")
    typer.echo(f"Catalogs: {', '.join(active_catalogs)}")
    typer.echo("")

    total_start = time.perf_counter()
    for stage in stages_to_run:
        stage_start = time.perf_counter()
        typer.echo(f"[{stage}] starting...")
        _run_stage(stage, cfg, catalog_filter)
        elapsed = time.perf_counter() - stage_start
        h, rem = divmod(int(elapsed), 3600)
        m, s = divmod(rem, 60)
        typer.echo(f"[{stage}] done in {h:02d}:{m:02d}:{s:02d}\n")

    total = time.perf_counter() - total_start
    h, rem = divmod(int(total), 3600)
    m, s = divmod(rem, 60)
    typer.echo(f"Pipeline complete. Total time: {h:02d}:{m:02d}:{s:02d}")


@app.command()
def notebook(
    port: int = typer.Option(8769, "--port", "-p", help="Port for the Jupyter notebook server."),
    login_node: str = typer.Option(
        "sdflogin003.slac.stanford.edu",
        "--login-node",
        help="SLAC login node hostname for the SSH tunnel.",
    ),
) -> None:
    """Start a Jupyter notebook server and print the SSH tunnel command to reach it."""
    user = os.environ.get("USER") or os.environ.get("LOGNAME") or "unknown"
    local_host = socket.gethostname().split(".")[0]
    jump_host = _detect_ssh_client_host()

    typer.echo("\nTo connect from your laptop, run:\n")
    if jump_host:
        typer.echo(
            f"  ssh -J {user}@{login_node},{user}@{jump_host} \\\n"
            f"      -L {port}:localhost:{port} \\\n"
            f"      {user}@{local_host}\n"
        )
    else:
        typer.echo(
            f"  ssh -J {user}@{login_node} \\\n"
            f"      -L {port}:localhost:{port} \\\n"
            f"      {user}@{local_host}\n"
        )
        typer.echo(
            "  (Could not detect intermediate jump host — SSH_CLIENT not set. "
            "Add the iana machine manually if needed.)\n"
        )

    typer.echo("Starting Jupyter...\n")
    subprocess.run(["jupyter", "notebook", "--no-browser", f"--port={port}"])


def _detect_ssh_client_host() -> Optional[str]:
    """Return the short hostname of the machine that SSH'd into this one, if detectable."""
    ssh_client = os.environ.get("SSH_CLIENT", "")
    if not ssh_client:
        return None
    client_ip = ssh_client.split()[0]
    try:
        fqdn = socket.gethostbyaddr(client_ip)[0]
        return fqdn.split(".")[0]
    except (socket.herror, OSError):
        # Fall back to the raw IP if reverse DNS fails
        return client_ip

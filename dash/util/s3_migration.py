#!/usr/bin/env python3
"""
Migrate data from S3 to a local directory, then remove the S3 sources.

If a --deadline (ISO-8601) is provided and the *newest* object
under all relevant prefixes was added strictly *after* the deadline,
then the migration is skipped.  The idea is to only migrate the files
if they are older than the provided deadline.

Notes:
- Only deletes S3 objects after a successful download of the corresponding prefix.
- Supports optional --dry-run to preview actions.
- Exits non‑zero on any failed transfer.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tqdm import tqdm
from typing import Iterable
from zoneinfo import ZoneInfo

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from boto3.s3.transfer import TransferConfig, S3Transfer


CATEGORIES = ("hats", "raw", "validation")
PACIFIC = ZoneInfo("US/Pacific")


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def parse_iso8601(ts: str) -> datetime:
    """Parse a permissive ISO-8601 string into an aware UTC datetime.

    Supports:
      - "Z" suffix
      - Offsets like "+07:00"
      - Naive datetimes are assumed to be US/Pacific
    """
    s = ts.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError as e:
        raise SystemExit(f"Invalid ISO-8601 deadline: {ts}") from e
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=PACIFIC)
    return dt.astimezone(timezone.utc)


@dataclasses.dataclass
class S3Object:
    key: str
    size: int
    last_modified: datetime  # timezone-aware


def list_objects(client, bucket: str, prefix: str) -> list[S3Object]:
    paginator = client.get_paginator("list_objects_v2")
    objects = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            # Skip "directory" placeholders
            key = item["Key"]
            if key.endswith("/") and item.get("Size", 0) == 0:
                continue
            lm = item["LastModified"]
            if lm.tzinfo is None:
                lm = lm.replace(tzinfo=timezone.utc)
            else:
                lm = lm.astimezone(timezone.utc)
            objects.append(
                S3Object(key=key, size=item.get("Size", 0), last_modified=lm)
            )
    return objects


def newest_last_modified(objs: Iterable[S3Object]) -> datetime | None:
    newest = None
    for obj in objs:
        if newest is None or obj.last_modified > newest:
            newest = obj.last_modified
    return newest


def relative_key_path(key: str, base_prefix: str) -> str:
    assert key.startswith(base_prefix)
    rel = key[len(base_prefix) :]
    return rel


def download_prefix(
    client,
    bucket: str,
    prefix: str,
    dest_dir: Path,
    *,
    max_concurrency: int,
    dry_run: bool,
) -> tuple[int, int]:
    """Download all objects under prefix to dest_dir.

    Returns (num_downloaded, num_failed)
    """
    transfer = S3Transfer(
        client,
        config=TransferConfig(
            max_concurrency=max_concurrency, multipart_threshold=16 * 1024 * 1024
        ),
    )
    objs = list_objects(client, bucket, prefix)

    if not objs:
        logging.info("No objects under s3://%s/%s", bucket, prefix)
        return 0, 0

    logging.info(
        "Planning to download %d objects (%.2f MiB) from s3://%s/%s to %s",
        len(objs),
        sum(obj.size for obj in objs) / 1024 / 1024,
        bucket,
        prefix,
        dest_dir,
    )

    dest_dir.mkdir(parents=True, exist_ok=True)

    if dry_run:
        for obj in tqdm(objs, unit="file"):
            logging.info(
                "DRY-RUN download -> %s", dest_dir / relative_key_path(obj.key, prefix)
            )
        return 0, 0

    def _download(obj: S3Object):
        f_downloaded = 0
        rel = relative_key_path(obj.key, prefix)
        target = dest_dir / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            transfer.download_file(bucket, obj.key, str(target))
            f_downloaded += 1
        except Exception as e:
            logging.error(
                "Failed to download s3://%s/%s -> %s: %s", bucket, obj.key, target, e
            )
            raise
        return f_downloaded

    downloaded = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrency) as pool:
        futures = [pool.submit(_download, obj) for obj in objs]
        for f in tqdm(
            concurrent.futures.as_completed(futures), total=len(futures), unit="file"
        ):
            try:
                f_downloaded = f.result()
                downloaded += f_downloaded
            except Exception:
                logging.info("Canceling undone futures")
                # Cancel remaining
                for other in futures:
                    other.cancel()
                break

    return downloaded


def delete_prefix(
    client, bucket: str, prefix: str, *, dry_run: bool, batch_size: int = 1000
) -> int:
    objs = list_objects(client, bucket, prefix)
    if not objs:
        logging.info("No objects to delete under s3://%s/%s", bucket, prefix)
        return 0

    if dry_run:
        for obj in tqdm(objs):
            logging.info("DRY-RUN delete -> s3://%s/%s", bucket, obj.key)
        return 0

    deleted = 0
    keys = [{"Key": obj.key} for obj in objs]
    for i in tqdm(list(range(0, len(keys), batch_size)), unit="batch"):
        batch = {"Objects": keys[i : i + batch_size], "Quiet": True}
        try:
            resp = client.delete_objects(Bucket=bucket, Delete=batch)
            deleted += len(resp.get("Deleted", []))
            errors = resp.get("Errors", [])
            for err in errors:
                logging.error(
                    "Failed to delete %s: %s", err.get("Key"), err.get("Message")
                )
        except (BotoCoreError, ClientError) as e:
            logging.error("Delete batch failed: %s", e)
            raise
    logging.info("Deleted %d objects under s3://%s/%s", deleted, bucket, prefix)
    return deleted


def migrate(
    s3_client,
    bucket: str,
    version: str,
    output_dir: Path,
    given_deadline: str,
    max_concurrency: int,
    dry_run: bool,
) -> int:
    """
    Migrates data from the given S3 bucket to the given output directory,
    returning the total number of overall failures.
    """
    plans = []
    for cat in CATEGORIES:
        prefix = f"{cat}/{version}/"
        dest = output_dir / cat / version
        plans.append((cat, prefix, dest))

    # Deadline gating across all prefixes
    deadline = parse_iso8601(given_deadline)
    logging.info("Deadline set to %s (UTC)", deadline.isoformat())
    newest_each = []
    for _, prefix, _ in plans:
        objs = list_objects(s3_client, bucket, prefix)
        newest = newest_last_modified(objs)
        if newest is not None:
            newest_each.append(newest)
            logging.info(
                "Newest under s3://%s/%s -> %s", bucket, prefix, newest.isoformat()
            )
        else:
            logging.info("No objects under s3://%s/%s", bucket, prefix)
    newest_overall = max(newest_each) if newest_each else None
    if newest_overall is None:
        logging.info("Nothing to migrate; skipping.")
        return 0
    if newest_overall > deadline:
        logging.info(
            "Newest object %s is newer than deadline %s; skipping migration.",
            newest_overall.isoformat(),
            deadline.isoformat(),
        )
        return 0

    # Perform migrations per prefix
    overall_failures = 0
    for cat, prefix, dest in plans:
        logging.info("Migrating %s to %s...", prefix, dest)
        try:
            downloaded, failed = download_prefix(
                s3_client,
                bucket,
                prefix,
                dest,
                max_concurrency=max_concurrency,
                dry_run=dry_run,
            )
            if dry_run:
                continue
            if failed:
                logging.error(
                    "%s: %d downloads failed; skipping delete for this prefix.",
                    prefix,
                    failed,
                )
                overall_failures += failed
                continue
            logging.info("%s: downloaded %d objects successfully.", prefix, downloaded)
            # Safe to delete this prefix now
            logging.info("Emptying %s from s3://%s/%s...", version, bucket, prefix)
            delete_prefix(s3_client, bucket, prefix, dry_run=dry_run)
        except Exception as e:
            logging.error("Migration failed for %s: %s", prefix, e)
            overall_failures += 1
            break

    return overall_failures


def main():
    parser = argparse.ArgumentParser(
        description="Migrate data from S3 to local with deadline gating.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--version", default=os.getenv("VERSION"), required=True, help="Version string"
    )
    parser.add_argument(
        "--output_dir",
        default=os.getenv("OUTPUT_DIR"),
        required=True,
        help="Local output directory root",
    )
    parser.add_argument("--bucket", default="rubin-lincc-hats", help="S3 bucket name")
    parser.add_argument("--profile", help="AWS profile name (optional)")

    # Compute default deadline, which is 1 month prior to today.
    default_deadline = (
        (datetime.today() - timedelta(days=30)).replace(tzinfo=PACIFIC).isoformat()
    )
    parser.add_argument(
        "--deadline",
        default=default_deadline,
        required=True,
        help="ISO-8601 timestamp; skip migration if newest object is after this",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show planned actions without executing them",
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=min(32, (os.cpu_count() or 4) * 4),
        help="Max concurrent transfers",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )
    args = parser.parse_args()

    setup_logging(args.log_level)

    # Prepare AWS client
    session = boto3.session.Session(profile_name=args.profile)
    s3_client = session.client(
        "s3", config=Config(retries={"max_attempts": 10, "mode": "adaptive"})
    )

    overall_failures = migrate(
        s3_client,
        args.bucket,
        args.version,
        Path(args.output_dir).resolve(),
        args.deadline,
        args.max_concurrency,
        args.dry_run,
    )

    if overall_failures:
        logging.error("Encountered %d overall failures.", overall_failures)
        raise SystemExit(1)


if __name__ == "__main__":
    main()

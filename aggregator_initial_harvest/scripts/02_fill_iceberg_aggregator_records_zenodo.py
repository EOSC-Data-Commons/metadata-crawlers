"""Create and fill the aggregator_records Iceberg table from an S3 bucket.

This table is used to later filter out a subset of records. Therefore it contains
less information, mainly ID, resource_type and dates created/updated. The second
table, records_to_harvest, will include full JSON alongside these columns.

The table is dropped and rebuilt on every run.

The table "aggregator_records" is created in the provided --namespace from a 
specified --bucket and the --prefix in that bucket (the default prefix is "zenodo_dump").

The run can be further customised by specifying the number of --workers to do work in 
parallel (default 32), and the number of records to be uploaded at a single time 
(--slice, default 10000).

Example:
    uv run python aggregator_initial_harvest/scripts/02_fill_iceberg_aggregator_records_zenodo.py --bucket zenodo20k-2026-10-03 --namespace test_harvest_ice
"""

import os

from dotenv import load_dotenv

# These variables need to be set so s3 > iceberg requests pass
os.environ.setdefault("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
os.environ.setdefault("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")

import argparse
import logging
from pathlib import Path

from aggregator_initial_harvest.apache_iceberg.iceberg_schemas import (
    SCHEMA_AGGREGATOR_RECORDS,
)
from aggregator_initial_harvest.apache_iceberg.zenodo_from_s3 import (
    build_catalog,
    fill_aggregator_records,
    recreate_table,
)
from aggregator_initial_harvest.s3.s3_upload import connect_to_s3

logger = logging.getLogger(__name__)

# This file lives in <repo root>/aggregator_initial_harvest/scripts/
_REPO_ROOT = Path(__file__).resolve().parents[2]

# Load env vars (S3/Iceberg credentials) before connecting to anything
load_dotenv(_REPO_ROOT / ".env", override=True)

def setup_logging() -> None:
    """
    Log messages from this project at INFO level and above, to the console.

    HTTP and AWS client libraries are kept at WARNING so their messages don't
    drown out our own.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s [%(threadName)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    for noisy in ("boto3", "botocore", "s3transfer", "urllib3", "pyiceberg.io"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

def parse_args() -> argparse.Namespace:
    """Define command-line arguments.

    --bucket and --namespace are mandatory.
    """
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bucket", required=True, help="Source S3 bucket containing the Zenodo JSON files")
    p.add_argument("--namespace", required=True, help="Iceberg namespace where the table will live")
    p.add_argument("--prefix", default="zenodo_dump", help="Key prefix inside the bucket (default: %(default)s)")
    p.add_argument("--slice", type=int, default=10000, help="Rows per commit (default: %(default)s)")
    p.add_argument("--workers", type=int, default=32, help="Parallel S3 reads (default: %(default)s)")
    return p.parse_args()

def require_env(name: str) -> str:
    """Read a mandatory environment variable, exiting with a hint if missing."""
    value = os.environ.get(name)
    if not value:
        raise SystemExit(
            f"{name} is not set. Add it to the .env file or the environment."
        )
    return value


def main():
    """Connect to S3 and the catalog, then rebuild and fill the aggregator records table."""
    setup_logging()
    args = parse_args()

    logger.info("Start of script %s.", __file__)

    # Full table name, including namespace
    identifier = f"{args.namespace}.aggregator_records"

    catalog = build_catalog(
        rest_uri=require_env("ICEBERG_REST_CATALOG"),
        s3_endpoint=require_env("S3_ENDPOINT"),
        access_key=require_env("S3_ACCESS_KEY"),
        secret_key=require_env("S3_SECRET_KEY"),
    )
    s3 = connect_to_s3(
        require_env("S3_ACCESS_KEY"),
        require_env("S3_SECRET_KEY"),
        require_env("S3_ENDPOINT"),
        max_pool_connections=args.workers,
    )

    logger.info("Source: s3://%s/%s", args.bucket, args.prefix)
    catalog.create_namespace_if_not_exists(args.namespace)

    recreate_table(catalog, identifier, SCHEMA_AGGREGATOR_RECORDS)
    fill_aggregator_records(
        catalog, s3, identifier,
        bucket=args.bucket,
        prefix=args.prefix,
        slice_size=args.slice,
        workers=args.workers,
    )


if __name__ == "__main__":
    main()
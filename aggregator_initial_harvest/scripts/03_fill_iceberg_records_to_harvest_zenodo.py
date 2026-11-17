"""Create and fill the records_to_harvest Iceberg table from a subset of aggregator_records.

The aggregator_records is filtered to only include resource_type = 'dataset'. (If needed, this
can be adjusted in the aggregator_initial_harvest.apache_iceberg.zenodo_from_s3.select_dataset_files() function.)
For these rows, they are inserted into the new tables, which also includes their full
raw_JSON, as well as two transformations of this JSON needed to conform to the form
"harvest_events" in the main metadata warehouse will expect.
Transformations:
    datacite_JSON: conforms with metadata_warehouse `harvest_events.raw_metadata` when "metadata_format" = "JSON"
    additional_metadata_JSON: conforms with metadata_warehouse `harvest_events.additional_metadata`

The table is dropped and rebuilt on every run.

The table "records_to_harvest" is created in the provided --namespace. The files are read
again from the --bucket since the full JSON info is needed. The bucket should be the same 
as the one used in script 02!
The prefix inside bucket is not needed, since it is already stored in the file column of
the aggregator_records table.

The run can be further customised by specifying the number of --workers to do work in 
parallel (default 32), and the number of records to be uploaded at a single time 
(--slice, default 5000. This is smaller than the first table since the rows here are larger).


Example:
    uv run python aggregator_initial_harvest/scripts/03_fill_iceberg_records_to_harvest_zenodo.py --bucket zenodo20k-2026-10-03 --namespace test_harvest_ice
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
    SCHEMA_RECORDS_TO_HARVEST,
)
from aggregator_initial_harvest.apache_iceberg.zenodo_from_s3 import (
    build_catalog,
    fill_records_to_harvest,
    recreate_table,
    select_dataset_files,
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
    p.add_argument("--namespace", required=True, help="Iceberg namespace where the tables live")
    p.add_argument("--slice", type=int, default=5000, help="Rows per commit (default: %(default)s)")
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
    """Connect to S3 and the catalog, then rebuild and fill the table."""
    setup_logging()
    args = parse_args()

    logger.info("Start of script %s.", __file__)
    
    # Full table names, including namespace
    t1_id = f"{args.namespace}.aggregator_records"
    t2_id = f"{args.namespace}.records_to_harvest"

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

    logger.info("Source: s3://%s", args.bucket)

    # Select the dataset records before recreating table
    # So a possible error doesn't empty the table.
    dataset_files = select_dataset_files(catalog, t1_id)

    recreate_table(catalog, t2_id, SCHEMA_RECORDS_TO_HARVEST)
    fill_records_to_harvest(
        catalog, s3, t2_id,
        bucket=args.bucket,
        dataset_files=dataset_files,
        slice_size=args.slice,
        workers=args.workers,
    )


if __name__ == "__main__":
    main()
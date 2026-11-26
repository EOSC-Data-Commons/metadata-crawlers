"""Send the records_to_harvest Iceberg table rows to the warehouse as harvest_events.

One harvest_event is created for every row of <namespace>.records_to_harvest
(filled by script 03). The events are sent in bulks of --bulk-size
(default 100) events per API request.

Before the events are sent, a harvest run is started with POST /harvest_run:
- the harvest_url is the Zenodo exporter API, the "endpoint" this harvest
  comes from. It must match the harvest_url of the Zenodo repository in the
  repositories table, so that the run resolves the repository's code
- the until date is the creation date of the dump the bucket was filled from,
  read from the dump_metadata/dump_metadata.json object script 01 uploaded

Each row becomes one event:
- record_identifier: the record's Zenodo id, just the number, e.g.
  "16647428" (the row's id column)
- datestamp: the record's modified (updated) date, falling back to created
- raw_metadata: the row's datacite_json
- additional_metadata: the row's additional_metadata_json
- repo_code: Zenodo's code in the repositories table, from the run response
- is_deleted: False
- is_json: True (the raw metadata is transformed Datacite JSON, not XML)

The run is closed at the end, successful only if every event was sent.

Example:
    uv run python aggregator_initial_harvest/scripts/04_harvest_zenodo_iceberg.py --bucket zenodo-metadata-dump --namespace harvest-iceberg
"""

import os

from dotenv import load_dotenv

# These variables need to be set so s3 > iceberg requests pass
os.environ.setdefault("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
os.environ.setdefault("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from aggregator_initial_harvest.apache_iceberg.zenodo_from_s3 import build_catalog
from aggregator_initial_harvest.s3.s3_upload import connect_to_s3, read_object
from harvester.db_api_functions import (
    close_harvest_run,
    close_warehouse_client,
    get_open_run_id,
    send_harvest_event_bulk,
    start_harvest_run,
)

logger = logging.getLogger(__name__)

# This file lives in <repo root>/aggregator_initial_harvest/scripts/
_REPO_ROOT = Path(__file__).resolve().parents[2]

# Load env vars (S3/Iceberg credentials, WAREHOUSE_API_URL) before connecting to anything
load_dotenv(_REPO_ROOT / ".env", override=True)

# The "endpoint" this harvest comes from: the Zenodo exporter API serving the
# metadata dumps. Must match the harvest_url of the Zenodo repository in the
# repositories table, so the started run resolves the repository's code.
ZENODO_EXPORTER_URL = "https://zenodo.org/oai2d"

# S3 object (uploaded by script 01) holding the exporter-API info of the dump
# the bucket was filled from; its "created" field is the run's until date
DUMP_METADATA_KEY = "dump_metadata/dump_metadata.json"


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
    """
    Define command-line arguments.

    --bucket and --namespace are mandatory.
    """
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--bucket",
        required=True,
        help="S3 bucket holding dump_metadata/dump_metadata.json",
    )
    p.add_argument(
        "--namespace",
        required=True,
        help="Iceberg namespace where the records_to_harvest table lives",
    )
    p.add_argument(
        "--bulk-size",
        type=int,
        default=100,
        help="Harvest events sent per API request (default: %(default)s)",
    )
    args = p.parse_args()
    if args.bulk_size < 1:
        p.error("--bulk-size must be at least 1")
    return args


def require_env(name: str) -> str:
    """Read a mandatory environment variable, exiting with a hint if missing."""
    value = os.environ.get(name)
    if not value:
        raise SystemExit(
            f"{name} is not set. Add it to the .env file or the environment."
        )
    return value


def load_dump_created_date(s3, bucket):
    """
    The creation date of the dump the bucket was filled from.

    Reads the exporter-API object script 01 stored as
    ``dump_metadata/dump_metadata.json`` and returns its ``created``
    field -- the until date of the harvest run started by this script.

    Args:
        s3: boto3 S3 client for the bucket.
        bucket (str): Bucket holding the dump metadata.

    Returns:
        str: ISO-8601 creation date of the dump.
    """
    dump_info = json.loads(read_object(s3, bucket, DUMP_METADATA_KEY))
    created = dump_info.get("created")
    if not created:
        raise RuntimeError(
            f"no created date in s3://{bucket}/{DUMP_METADATA_KEY}: {dump_info!r}"
        )
    return created


def build_event_payload(row, repo_code, harvest_run_id):
    """
    Build one harvest_event payload from a records_to_harvest row.

    The record's Zenodo id is the identifier; its modified date is the
    datestamp; and the two JSON transforms stored by script 03 are the
    raw and additional metadata. The datacite_json itself is sent
    untouched, byte for byte.

    Args:
        row (dict): One records_to_harvest row, as an arrow row dict.
        repo_code (str): Zenodo's code in the repositories table.
        harvest_run_id: Identifier of the current harvest run.

    Returns:
        dict | None: The event payload, or None if the row cannot become
        an event (no id, no datacite_json, or no date at all) -- the row
        is then counted as skipped.
    """
    record_id = row.get("id")
    if record_id is None:
        logger.warning("row without id, skipping: %r", row.get("file"))
        return None
    raw_metadata = row.get("datacite_json")
    if raw_metadata is None:
        logger.warning("record %s has no datacite_json, skipping", record_id)
        return None
    datestamp = row.get("updated") or row.get("created")
    if datestamp is None:
        logger.warning("record %s has no updated or created date, skipping", record_id)
        return None
    return {
        "record_identifier": str(record_id),
        "datestamp": datestamp.isoformat(),
        "raw_metadata": raw_metadata,
        "additional_metadata": row.get("additional_metadata_json"),
        "harvest_url": ZENODO_EXPORTER_URL,
        "repo_code": repo_code,
        "harvest_run_id": harvest_run_id,
        "is_deleted": False,
        "is_json": True,
    }


def iter_row_groups(reader, group_size):
    """
    Yield the rows of a RecordBatchReader in lists of up to group_size.

    Iceberg hands out record batches of whatever size the data files
    use; this regroups them into fixed-size lists, one per bulk
    request, so the whole table never has to sit in memory.

    Args:
        reader (pyarrow.RecordBatchReader): Batches of records_to_harvest rows.
        group_size (int): Rows per group.

    Yields:
        list[dict]: Up to group_size rows, as dictionaries.
    """
    group = []
    for batch in reader:
        for row in batch.to_pylist():
            group.append(row)
            if len(group) >= group_size:
                yield group
                group = []
    if group:
        yield group


def main():
    """Start a harvest run and send every records_to_harvest row as a harvest event."""
    setup_logging()
    args = parse_args()

    logger.info("Start of script %s.", __file__)
    logger.debug(
        "Arguments: bucket=%s, namespace=%s, bulk_size=%s",
        args.bucket,
        args.namespace,
        args.bulk_size,
    )

    table_id = f"{args.namespace}.records_to_harvest"

    # Check necessary connections, so that an error stops the run from initiating
    s3 = connect_to_s3(
        require_env("S3_ACCESS_KEY"),
        require_env("S3_SECRET_KEY"),
        require_env("S3_ENDPOINT"),
    )
    until_date = load_dump_created_date(s3, args.bucket)
    logger.info("Dump created %s, using it as the harvest run until date.", until_date)

    catalog = build_catalog(
        rest_uri=require_env("ICEBERG_REST_CATALOG"),
        s3_endpoint=require_env("S3_ENDPOINT"),
        access_key=require_env("S3_ACCESS_KEY"),
        secret_key=require_env("S3_SECRET_KEY"),
    )
    table = catalog.load_table(table_id)
    n_rows = table.scan(selected_fields=("id",)).count()
    logger.info("%s holds %s rows to send as harvest events.", table_id, f"{n_rows:,}")

    reader = table.scan(
        selected_fields=(
            "id",
            "created",
            "updated",
            "datacite_json",
            "additional_metadata_json",
        ),
    ).to_arrow_batch_reader()

    start_time = datetime.now(timezone.utc).isoformat(timespec="seconds")
    harvest_run_id = None
    success = False

    try:
        run_info = start_harvest_run(ZENODO_EXPORTER_URL, until_date=until_date)
        if run_info is None:
            # An open run for the endpoint blocks a new one: close it as
            # failed and start over, like the regular harvester does.
            logger.error("Failed to start harvest run, checking for existing open run...")
            open_run_id = get_open_run_id(ZENODO_EXPORTER_URL)
            if open_run_id:
                logger.warning("Closing existing open harvest run %s as failed", open_run_id)
                close_harvest_run(
                    {
                        "id": open_run_id,
                        "success": False,
                        # this overwrites the started_at date already in the
                        # DB, but the API requires the field
                        "started_at": start_time,
                        "completed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    }
                )
                logger.info("Retry to start a new harvest run...")
                run_info = start_harvest_run(ZENODO_EXPORTER_URL, until_date=until_date)
            if run_info is None:
                logger.error("Cannot start a harvest run. Quitting.")
                return

        harvest_run_id = run_info["id"]
        config = run_info.get("endpoint_config")
        if not config:
            raise ValueError("Missing endpoint_config in API response")
        repo_code = config.get("code")
        if repo_code is None:
            raise ValueError("Missing code of repository in endpoint config")
        logger.info("Started harvest run id=%s for repo code %s.", harvest_run_id, repo_code)

        sent = failed = skipped = 0
        next_progress_log = 10000
        for rows in iter_row_groups(reader, args.bulk_size):
            events = []
            for row in rows:
                event = build_event_payload(row, repo_code, harvest_run_id)
                if event is None:
                    skipped += 1
                else:
                    events.append(event)
            if not events:
                continue
            if send_harvest_event_bulk(events):
                sent += len(events)
            else:
                failed += len(events)
            if sent + failed + skipped >= next_progress_log:
                logger.info(
                    "Progress: %s rows processed, %s events sent, %s failed, %s skipped",
                    f"{sent + failed + skipped:,}",
                    f"{sent:,}",
                    f"{failed:,}",
                    f"{skipped:,}",
                )
                next_progress_log += 10_000

        success = failed == 0
        logger.info(
            "Harvest summary: processed %s rows, sent %s events, "
            "failed to send %s events, skipped %s rows",
            f"{sent + failed + skipped:,}",
            f"{sent:,}",
            f"{failed:,}",
            f"{skipped:,}",
        )

    except Exception as e:
        logger.exception("Harvest encountered an error: %s", e)
        success = False

    finally:
        if harvest_run_id is not None:
            close_harvest_run(
                {
                    "id": harvest_run_id,
                    "success": success,
                    "started_at": start_time,
                    "completed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                }
            )
        close_warehouse_client()


if __name__ == "__main__":
    main()

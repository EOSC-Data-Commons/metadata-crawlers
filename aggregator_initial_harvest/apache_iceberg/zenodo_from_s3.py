"""Fill the Zenodo Iceberg tables from JSON records stored in an S3 bucket.

Two tables are involved (schemas in ``aggregator_initial_harvest/apache_iceberg/iceberg_schemas.py``):

``aggregator_records`` -- one slim row per Zenodo record: id, source,
  resource type and dates; so we can select which type of records we'll 
  want to harvest.
``records_to_harvest`` -- the ``resource_type = 'dataset'`` subset, with
  the full record text and its DataCite transform.

This file contains functions for managing and filling the tables, 
as well as specific function for filling the tables based on s3
contents, including transformation of raw JSON to datacite JSON for   
``records_to_harvest``.
"""

import json
import logging
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from itertools import islice
from typing import NamedTuple

import pyarrow as pa
from pyiceberg.catalog import load_catalog

from aggregator_initial_harvest.apache_iceberg.iceberg_schemas import (
    SCHEMA_AGGREGATOR_RECORDS,
    SCHEMA_RECORDS_TO_HARVEST,
)
from aggregator_initial_harvest.json_manipulation.json_transform import (
    transform_json_zenodo_to_datacite,
    transform_json_zenodo_to_files_endpoint,
)
from aggregator_initial_harvest.s3.s3_upload import iter_s3_keys, read_object

logger = logging.getLogger(__name__)

def build_catalog(rest_uri, s3_endpoint, access_key, secret_key, name="remote"):
    """Build a pyiceberg REST catalog.

    Catalog metadata is served by the REST server at ``rest_uri``.

    Args:
        rest_uri (str): URL of the Iceberg REST catalog server.
        s3_endpoint (str): URL of the S3 endpoint holding the table files.
        access_key (str): S3 access key id.
        secret_key (str): S3 secret access key.
        name (str): Catalog name, used by pyiceberg for lookups.

    Returns:
        A connected pyiceberg REST catalog.
    """
    props = {
        "type": "rest",
        "uri": rest_uri,
        "s3.endpoint": s3_endpoint,
        "s3.access-key-id": access_key,
        "s3.secret-access-key": secret_key,
        # Explicit region: without it, pyiceberg tries to auto-detect the
        # bucket's region by asking real AWS S3, which fails (and logs
        # "Unable to resolve region for bucket ...") for a bucket that only
        # exists on our private S3 endpoint. us-east-1 is also what pyarrow
        # falls back to when no region is given.
        "s3.region": "us-east-1",
        "rest.client.max-retries": 6,
        "rest.client.connection-timeout-ms": 60000,
    }
    return load_catalog(name, **props)


def recreate_table(catalog, identifier, schema):
    """Drop ``identifier`` (with its data) and recreate it empty.

    For recreating from stratch.

    Args:
        catalog: pyiceberg catalog holding the table.
        identifier (str): Fully qualified table identifier,
            e.g. ``"namespace.records_to_harvest"``.
        schema (pyiceberg.schema.Schema): Schema for the new table.

    Returns:
        The freshly created pyiceberg table.
    """
    if catalog.table_exists(identifier):
        catalog.purge_table(identifier)
    return catalog.create_table(identifier, schema=schema)


def append_with_retry(table, arrow_tbl, attempts=4):
    """Append ``arrow_tbl`` to ``table``, retrying failed commits.

    A pyiceberg commit is a POST request, which pyiceberg's built-in REST
    retries do not cover, so dropped connections are retried here. 
    After each failure the table is refreshed to check
    whether the commit actually landed despite the error -- if it did,
    retrying would append the same rows twice, so the append counts as done.

    Args:
        table: pyiceberg table to append to.
        arrow_tbl (pyarrow.Table): Rows to append, carrying the table schema.
        attempts (int): Total number of attempts; an exception is
            raised if all of them fail.

    Returns:
        None
    """
    before = len(table.metadata.snapshots)
    for i in range(attempts):
        try:
            table.append(arrow_tbl)
            return
        except Exception as e:
            if i == attempts - 1:
                raise
            logger.warning("append failed (%r), retrying in %ds", e, 2 ** i)
            # Exponential backoff, wait longer each attempt
            time.sleep(2 ** i)
            table.refresh()
            if len(table.metadata.snapshots) > before:
                logger.warning("commit had actually landed, not retrying")
                return


def count_rows(catalog, identifier):
    """Count the rows currently in a table, from a fresh scan.

    ``load_table`` reloads the table metadata, so the count reflects the
    latest snapshot -- e.g. the state right after a fill finished.
    """
    table = catalog.load_table(identifier)
    return table.scan(selected_fields=("file",)).to_arrow().num_rows


def to_date(s):
    """Date part of an ISO-8601 timestamp string.

    Zenodo records carry full timestamps (``2020-01-02T03:04:05``), while the
    tables only keep the day. Empty strings and ``None`` pass through as
    ``None``.

    Args:
        s (str | None): Timestamp string, e.g. a record's ``created``.

    Returns:
        datetime.date | None: The parsed date.
    """
    return date.fromisoformat(s[:10]) if s else None


def slim_row_from_dict(key, d):
    """Build the columns shared by both tables from a parsed Zenodo record.

    Args:
        key (str): S3 object key the record was read from; stored verbatim
            in the ``file`` column.
        d (dict): Parsed Zenodo record.

    Returns:
        dict | None: A row with the ``SCHEMA_AGGREGATOR_RECORDS`` columns,
        or ``None`` if the record has no ``id`` (a required column).
    """
    if d.get("id") is None:
        return None  # id is a required column
    return {
        "file": key,  # full S3 key
        "id": str(d["id"]),
        # Source will be the same for every row --
        # useful if we onboard additional aggregators later
        "source": "Zenodo",
        "resource_type": (d.get("metadata", {}).get("resource_type") or {}).get("id"),
        "created": to_date(d.get("created")),
        "updated": to_date(d.get("updated")),
    }


def harvest_row_from_json(key, raw_json):
    """Build a full ``records_to_harvest`` row from a raw Zenodo record.

    Parses ``raw_json`` and extends :func:`slim_row_from_dict` with:

    * ``raw_json`` -- the exact text of the JSON file, so the original record
      can always be recovered from the table;
    * ``datacite_json`` -- the record transformed to the DataCite schema
    * ``additional_metadata_json`` -- the record transformed to the /files schema

    Args:
        key (str): S3 object key the record was read from.
        raw_json (str): The exact text of the JSON record; parsed for the
            other columns and stored verbatim in ``raw_json``.

    Returns:
        dict | None: A full row, or ``None`` if the record has no ``id``.
    """
    d = json.loads(raw_json)
    row = slim_row_from_dict(key, d)
    if row is None:
        return None
    row["raw_json"] = raw_json
    row["datacite_json"] = transform_json_zenodo_to_datacite(d)
    row["additional_metadata_json"] = transform_json_zenodo_to_files_endpoint(d)
    return row


class FillStats(NamedTuple):
    """Outcome of one table fill.

    Attributes:
        processed (int): S3 keys consumed from the source.
        appended (int): Rows committed to the table.
        skipped (int): Keys that produced no row -- the record had no
            ``id``, or reading/parsing it raised an error.
    """

    processed: int
    appended: int
    skipped: int


def _sliced_fill(keys, build_row, catalog, identifier, schema, slice_size, workers):
    """Helper for creating rows and uploading data to tables in parallel.

    Builds one row per key with a pool of ``workers`` threads and commits
    the rows to the table in slices of ``slice_size``. 
    
    ``build_row`` will be defined in functions for working with specific 
    tables. It must catch errors and return ``None`` for a key that 
    produced no row, e.g. has a nonsupported format.

    Args:
        keys (Iterable[str]): S3 keys to process, in commit order.
        build_row (Callable[[str], dict | None]): Builds a row for a key;
            a ``None`` result is counted as skipped.
        catalog: pyiceberg catalog holding the table.
        identifier (str): Table identifier to fill.
        schema (pyiceberg.schema.Schema): Schema of the table to fill.
        slice_size (int): Rows per commit.
        workers (int): Number of workers, parallel S3 reads.

    Returns:
        FillStats: What was processed, appended and skipped.
    """
    table = catalog.load_table(identifier)
    arrow_schema = schema.as_arrow()
    processed = appended = skipped = 0

    def commit(rows, last_key):
        # accumulate values in these variables in the main _sliced_fill function
        nonlocal appended, skipped
        good = [r for r in rows if r is not None]
        skipped += len(rows) - len(good)
        if good:
            append_with_retry(table, pa.Table.from_pylist(good, schema=arrow_schema))
        appended += len(good)
        logger.info(
            "%s: committed %s rows (%s skipped), last key %s",
            identifier, f"{appended:,}", f"{skipped:,}", last_key,
        )

    # One slice of tasks in a queue, at least a couple per worker.
    # The pool keeps working while the main thread commits a finished slice.
    lookahead = max(slice_size, 2 * workers)
    keys_iter = iter(keys)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        fifo_queue = deque(
            (ex.submit(build_row, key), key) for key in islice(keys_iter, lookahead)
        )
        rows, last_key = [], None
        while fifo_queue:
            fut, key = fifo_queue.popleft()
            for k in islice(keys_iter, 1):
                # new thread to replace the popped one
                fifo_queue.append((ex.submit(build_row, k), k))
            rows.append(fut.result())
            last_key = key
            processed += 1
            if len(rows) >= slice_size:
                commit(rows, last_key)
                rows = []
        if rows:
            commit(rows, last_key)

    stats = FillStats(processed, appended, skipped)
    log_fill_summary(catalog, identifier, stats)
    return stats


def log_fill_summary(catalog, identifier, stats):
    """Log the row count of a freshly filled table, warning on mismatches.

    Compares the rows now in the table with the rows appended this run;
    a difference means rows were lost or left over (e.g. from an earlier
    run or a lost commit) and is reported as a warning.
    """
    n = count_rows(catalog, identifier)
    logger.info(
        "%s done: table has %s rows (%s processed, %s appended, %s skipped)",
        identifier, f"{n:,}", f"{stats.processed:,}", f"{stats.appended:,}", f"{stats.skipped:,}",
    )
    if n != stats.appended:
        logger.warning(
            "%s row count (%s) differs from rows appended this run (%s)",
            identifier, n, stats.appended,
        )


# Filling the specific tables

def fill_aggregator_records(catalog, s3, identifier, bucket, prefix, slice_size, workers):
    """Fill ``aggregator_records`` from every JSON in the dump.

    One row per ``*.json`` object under ``s3://{bucket}/{prefix}``
    If any records don't have an id, they are skipped and logged.
    The table must already exist -- :func:`recreate_table` sets it up.

    Args:
        catalog: pyiceberg catalog holding the table.
        s3: boto3 S3 client for the source bucket.
        identifier (str): Table identifier of ``aggregator_records``.
        bucket (str): Source S3 bucket.
        prefix (str): Key prefix inside the bucket.
        slice_size (int): Rows per commit.
        workers (int): Parallel S3 reads.

    Returns:
        FillStats: What was processed, appended and skipped.
    """
    def build_row(key):
        try:
            return slim_row_from_dict(key, json.loads(read_object(s3, bucket, key)))
        except Exception:
            logger.exception("failed to build a row for %s, skipping", key)
            return None

    return _sliced_fill(
        iter_s3_keys(s3, bucket, prefix), build_row,
        catalog, identifier, SCHEMA_AGGREGATOR_RECORDS, slice_size, workers,
    )


def select_dataset_files(catalog, identifier):
    """Filter ``aggregator_records`` to only dataset records.

    Loads the table fresh (so the scan sees the just-finished fill), scans
    for ``resource_type = 'dataset'`` and returns the sorted values of the
    ``file`` column: the S3 keys to copy into ``records_to_harvest``.

    Args:
        catalog: pyiceberg catalog holding ``aggregator_records``.
        identifier (str): Table identifier of ``aggregator_records``.

    Returns:
        list[str]: Sorted S3 keys of dataset records.
    """
    src = catalog.load_table(identifier)
    t = src.scan(
        row_filter="resource_type = 'dataset'",
        selected_fields=("file",),
    ).to_arrow()
    files = sorted(t["file"].to_pylist())
    logger.info("%s dataset records to copy", f"{len(files):,}")
    return files


def fill_records_to_harvest(catalog, s3, identifier, bucket, dataset_files, slice_size, workers):
    """Fill ``records_to_harvest`` from the selected dataset records.

    Every key in ``dataset_files`` is read from S3 again and stored 
    as an expanded row with the original JSON and two transformed 
    JSON versions.
    
    The table must already exist -- :func:`recreate_table` sets it up.

    Args:
        catalog: pyiceberg catalog holding the table.
        s3: boto3 S3 client for the source bucket.
        identifier (str): Table identifier of ``records_to_harvest``.
        bucket (str): Source S3 bucket the keys live in.
        dataset_files (list[str]): S3 keys to copy, should be the 
            result of `select_dataset_files`.
        slice_size (int): Rows per commit.
        workers (int): Parallel S3 reads.

    Returns:
        FillStats: What was processed, appended and skipped.
    """
    def build_row(key):
        try:
            return harvest_row_from_json(key, read_object(s3, bucket, key))
        except Exception:
            logger.exception("failed to build a row for %s, skipping", key)
            return None

    return _sliced_fill(
        dataset_files, build_row,
        catalog, identifier, SCHEMA_RECORDS_TO_HARVEST, slice_size, workers,
    )
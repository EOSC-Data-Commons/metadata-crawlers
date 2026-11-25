# Download latest Zenodo dump and push it to S3 bucket

# The Zenodo download takes ~13 GB on disk! The file is needed for streaming records
# to S3, after which it can be deleted. The download can last a few minutes.

# Uploading is done in parallel with multiple workers (--workers, default 16) -
# can take multiple hours locally to process all records!

# The info of the chosen dump (its object from the exporter API) is also uploaded
# to the bucket as dump_metadata/dump_metadata.json, so the dump version and its
# creation date can be queried later.

# After the records are uploaded, the latest records-deleted.csv.gz is downloaded
# and every record_id it lists is deleted from the bucket, so records Zenodo
# removed don't linger in the bucket. Since we can't easily wipe the bucket and start
# from stratch, we use this list of all records that were ever deleted from Zenodo to
# make sure no invalid files remain in the bucket
#
# Note: the number logged for the deletion step is the number of record IDs whose
# deletion requests succeeded, NOT the number of objects that actually existed in the
# bucket and were removed -- S3 does not report whether a deleted key existed.

# --bucket Mandatory! S3 bucket to upload the dump to.
# --local-file defines the filepath of the .tar.gz to download. Start at project root.
# --deleted-file defines the filepath of the records-deleted.csv.gz to download.
# --cleanup-local-dump can be set to "yes" to delete the downloaded dump .tar.gz and
# records-deleted.csv.gz after the script finishes


import argparse
import csv
import gzip
import json
import logging
import os
import tarfile
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from aggregator_initial_harvest.s3.s3_upload import (
    UploadStats,
    connect_to_s3,
    download_verified,
    upload,
)

logger = logging.getLogger(__name__)

# This file lives in <repo root>/aggregator_initial_harvest/scripts/
_REPO_ROOT = Path(__file__).resolve().parents[2]

# S3 key where we'll store the exporter-API info of the dump the bucket was 
# filled from
DUMP_METADATA_KEY = "dump_metadata/dump_metadata.json"

# Setup secrets, target bucket, local file to download dump, number of workers
load_dotenv(_REPO_ROOT / ".env", override=True)

ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")


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
    for noisy in ("boto3", "botocore", "s3transfer", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def parse_args() -> argparse.Namespace:
    """
    Define command-line arguments.
    The only mandatory is --bucket, the S3 bucket to create/upload to.
    """
    parser = argparse.ArgumentParser(
        description="Download the newest Zenodo dump and upload it to S3."
    )
    parser.add_argument("--bucket", required=True, help="destination S3 bucket")
    parser.add_argument(
        "--local-file",
        default="records-json.tar.gz",
        help="file path where the downloaded dump will be stored",
    )
    parser.add_argument(
        "--deleted-file",
        default="records-deleted.csv.gz",
        help="file path where the downloaded deleted-records CSV will be stored",
    )
    parser.add_argument(
        "--cleanup-local-dump",
        choices=["yes", "no"],
        default="no",
        help="yes/no: delete the local dump after a successful upload",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=16,
        help="Parallel S3 uploads (default: %(default)s)",
    )
    return parser.parse_args()


def upload_dump_metadata(s3_client, bucket, dump_info):
    """
    Upload the exporter-API info of the processed dump as JSON to the bucket.

    Stores the whole object the exporter API returned for the chosen dump
    (creation date, size, checksum, links), so the bucket always records
    which dump version it was filled from.

    Args:
        s3_client: boto3 S3 client for the bucket.
        bucket (str): Destination bucket name.
        dump_info (dict): Exporter-API object of the chosen dump.
    """
    s3_client.put_object(
        Bucket=bucket,
        Key=DUMP_METADATA_KEY,
        Body=json.dumps(dump_info, indent=2),
        ContentType="application/json",
    )
    logger.info("Dump metadata uploaded to s3://%s/%s", bucket, DUMP_METADATA_KEY)


def newest_exporter_version(exporter, filename):
    """
    Newest version entry of an exporter file, by creation date.

    The exporter API lists every published version of each dump file;
    the newest one is the current head.

    Args:
        exporter (dict): Parsed JSON of the exporter API response.
        filename (str): Exporter file whose versions to pick from,
            e.g. "records-json.tar.gz".

    Returns:
        dict: The version object of the newest version.
    """
    return max(exporter[filename], key=lambda v: datetime.fromisoformat(v["created"]))


def iter_deleted_record_ids(path):
    """
    Yield the record_id of every row of the records-deleted CSV.

    The CSV is decompressed and parsed row by row, so a list with
    millions of deletions never sits fully in memory. 
    
    IF the record_id column is missing (the csv format changed) 
    raises an exception instead of silently deleting nothing.

    Args:
        path (str): Local path of the downloaded records-deleted.csv.gz.

    Yields:
        str: record_id of one deleted record.
    """
    with gzip.open(path, "rt", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "record_id" not in reader.fieldnames:
            raise RuntimeError(
                f"no record_id column in {path} (columns: {reader.fieldnames})"
            )
        for row in reader:
            record_id = (row.get("record_id") or "").strip()
            if record_id:
                yield record_id


def delete_deleted_records(s3_client, bucket, prefix, csv_path, workers, batch=1000):
    """
    Delete the bucket objects of all records in the records-deleted CSV.

    For every record_id in the CSV, "<prefix><record_id>.json" is deleted
    from the bucket. Deletes are sent as parallel batch requests of
    ``batch`` keys (1000 is the S3 maximum per request). 
    
    S3 deleting a key that does not exist is a no-op -- so
    record IDs that were never uploaded are simply skipped, providing
    "delete it if it exists" behavior.

    A failed request is logged and counted, and the run continues with
    the remaining batches.

    NOTE: the success count is the number of deletion requests that
    succeeded, not the number of objects that actually existed in the
    bucket and were removed. S3 does not report whether a deleted key
    existed, so record IDs that were never uploaded are included in
    the count; only the failure count is exact.

    Args:
        s3_client: boto3 S3 client for the bucket.
        bucket (str): Bucket to delete the records from.
        prefix (str): Key prefix the record objects live under,
            e.g. "zenodo_dump/".
        csv_path (str): Local path of the records-deleted.csv.gz.
        workers (int): Number of parallel delete requests.
        batch (int): Keys per delete request, capped at 1000.

    Returns:
        tuple[int, int]: Keys covered by successful delete requests and
        keys that failed to delete. The first number is NOT the count
        of objects that were actually present and removed -- see the
        note above.
    """
    batch = min(batch, 1000)

    def delete_batch(keys):
        try:
            resp = s3_client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": k} for k in keys], "Quiet": True},
            )
            errors = resp.get("Errors") or []
            for e in errors:
                logger.warning(
                    "failed to delete %s: %s", e.get("Key"), e.get("Message")
                )
            return len(keys) - len(errors), len(errors)
        except Exception:
            logger.exception("delete request for %d keys failed", len(keys))
            return 0, len(keys)

    succeeded = failed = 0
    requested = 0
    keys = []
    # Same futures pattern as the upload: the main thread reads the CSV
    # and keeps at most 2 * workers delete requests in flight, so memory
    # stays bounded no matter how long the deletion list is.
    lookahead = 2 * workers
    with ThreadPoolExecutor(max_workers=workers) as ex:
        fifo_queue = deque()
        for record_id in iter_deleted_record_ids(csv_path):
            keys.append(f"{prefix}{record_id}.json")
            requested += 1
            if requested % 100_000 == 0:
                logger.info(
                    "Deletion progress: %s keys requested", f"{requested:,}"
                )
            if len(keys) >= batch:
                fifo_queue.append(ex.submit(delete_batch, keys))
                keys = []
                if len(fifo_queue) >= lookahead:
                    # Wait for the oldest request so memory stays bounded
                    d, f = fifo_queue.popleft().result()
                    succeeded += d
                    failed += f
        if keys:
            fifo_queue.append(ex.submit(delete_batch, keys))
        # Wait for the requests still in flight
        while fifo_queue:
            d, f = fifo_queue.popleft().result()
            succeeded += d
            failed += f
    return succeeded, failed


def main():
    """Download the newest Zenodo dump, upload it to S3, then delete the Zenodo-deleted records from the bucket."""
    setup_logging()
    args = parse_args()

    logger.info("Start of script %s.", __file__)
    logger.debug(
        "Arguments: bucket=%s, local_file=%s, deleted_file=%s, cleanup_local_dump=%s",
        args.bucket,
        args.local_file,
        args.deleted_file,
        args.cleanup_local_dump,
    )

    BUCKET = args.bucket
    PREFIX = "zenodo_dump/"

    # We'll first download the file locally, then stream it to S3. Otherwise direct streaming fails
    LOCAL_FILE = args.local_file

    # The deleted-records CSV is also downloaded locally before parsing
    DELETED_LOCAL_FILE = args.deleted_file

    # Stream to s3 in parallel
    WORKERS = args.workers

    # Headers identifying us as the requester
    requester_email = os.environ.get("REQUESTER_EMAIL")
    if not requester_email:
        raise SystemExit(
            "REQUESTER_EMAIL is not set. Add it to the .env file or the "
            "environment (it identifies us to Zenodo in the User-Agent header)."
        )
    HEADERS = {
        "User-Agent": f"zenodo-iceberg-loader(v0.1) / {requester_email}",
    }

    logger.debug("Endpoint: %s, prefix: %s, workers: %d", S3_ENDPOINT, PREFIX, WORKERS)

    s3_client = connect_to_s3(
        ACCESS_KEY, SECRET_KEY, S3_ENDPOINT, max_pool_connections=WORKERS
    )

    # Create bucket if it doesn't already exist

    try:
        s3_client.head_bucket(Bucket=BUCKET)
        logger.info("Bucket %s already exists.", BUCKET)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            s3_client.create_bucket(Bucket=BUCKET)
            logger.info("Bucket %s created.", BUCKET)
        else:
            logger.error("Could not access bucket %s: %s", BUCKET, e)
            raise

    # Zenodo dump ---
    # Download the latest version of the JSON metadata dump
    # and of the deleted-records list

    resp = requests.get("https://zenodo.org/api/exporter", timeout=30, headers=HEADERS)
    resp.raise_for_status()

    # Every dump file the exporter offers, with all of its versions
    exporter = resp.json()

    newest_dump = newest_exporter_version(exporter, "records-json.tar.gz")
    newest_deleted = newest_exporter_version(exporter, "records-deleted.csv.gz")

    zenodo_metadata_url = newest_dump["links"]["self"]  # URL to download the dump
    zenodo_metadata_date = newest_dump["created"]

    # Expected hash for validation, as well as expectedfilesize
    EXPECTED_MD5 = newest_dump["checksum"].removeprefix("md5:")
    EXPECTED_SIZE = int(newest_dump["size"])

    # The deleted-records CSV: URL, creation date, checksum and size
    deleted_url = newest_deleted["links"]["self"]
    deleted_date = newest_deleted["created"]
    EXPECTED_DELETED_MD5 = newest_deleted["checksum"].removeprefix("md5:")
    EXPECTED_DELETED_SIZE = int(newest_deleted["size"])

    logger.info(
        "Latest metadata dump: %s (created %s)",
        zenodo_metadata_url,
        zenodo_metadata_date,
    )
    logger.debug(
        "Expected size: %d bytes, expected md5: %s", EXPECTED_SIZE, EXPECTED_MD5
    )
    logger.info(
        "Latest deleted-records list: %s (created %s)",
        deleted_url,
        deleted_date,
    )
    logger.debug(
        "Expected size: %d bytes, expected md5: %s",
        EXPECTED_DELETED_SIZE,
        EXPECTED_DELETED_MD5,
    )

    # Download file locally
    # Lengthy!
    logger.info("Downloading dump to %s", LOCAL_FILE)
    download_verified(
        zenodo_metadata_url, LOCAL_FILE, EXPECTED_MD5, EXPECTED_SIZE, HEADERS
    )

    # Store the info of the dump we just verified, before the lengthy record
    # upload: even if that upload dies partway, the bucket still records
    # which dump version the records came from.
    upload_dump_metadata(s3_client, BUCKET, newest_dump)

    # Upload to s3 in parallel. Same futures-based pattern as the fill scripts:
    # the main thread reads the (sequential) tar stream and keeps at most
    # 2 * WORKERS uploads in flight -- one record each in memory -- waiting
    # on the oldest future before queueing more.
    stats = UploadStats()
    lookahead = 2 * WORKERS

    logger.info("Starting upload to S3.")

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        with tarfile.open(LOCAL_FILE, mode="r|gz") as tar:
            fifo_queue = deque()
            for member in tar:
                if not member.isfile():
                    logger.debug("Skipping non-file archive member: %s", member.name)
                    continue
                data = tar.extractfile(member).read()
                if len(fifo_queue) >= lookahead:
                    # upload() never raises, this only waits for the oldest
                    # upload to finish so memory stays bounded
                    fifo_queue.popleft().result()
                fifo_queue.append(
                    pool.submit(
                        upload, s3_client, BUCKET, PREFIX + member.name, data, stats
                    )
                )
            # Wait for the uploads still in flight
            while fifo_queue:
                fifo_queue.popleft().result()

    logger.info("Done: %d uploaded, %d failed", stats.uploaded, stats.failed)

    # Deleted records ---
    # Remove the objects of records Zenodo deleted, so the bucket ends up
    # holding the dump minus those records. 
    # Since we re-use an existing bucket, a record being in an old dump would
    # persist even though the new dump does not include it.
    logger.info("Downloading deleted-records list to %s", DELETED_LOCAL_FILE)
    download_verified(
        deleted_url,
        DELETED_LOCAL_FILE,
        EXPECTED_DELETED_MD5,
        EXPECTED_DELETED_SIZE,
        HEADERS,
    )
    delete_succeeded, delete_failed = delete_deleted_records(
        s3_client, BUCKET, PREFIX, DELETED_LOCAL_FILE, WORKERS
    )
    logger.info(
        "Deletion done: deletion requests succeeded for %s keys, %s failed",
        f"{delete_succeeded:,}",
        f"{delete_failed:,}",
    )
    if delete_failed:
        logger.warning(
            "%s keys could not be deleted; deletions are idempotent, so "
            "re-running the script retries them",
            f"{delete_failed:,}",
        )

    # Optionally: if indicated, delete the local dump file to free up space
    if args.cleanup_local_dump == "yes":
        os.remove(LOCAL_FILE)
        os.remove(DELETED_LOCAL_FILE)
        logger.info("Deleted %s and %s.", LOCAL_FILE, DELETED_LOCAL_FILE)

    logger.info("Script %s complete.", __file__)


if __name__ == "__main__":
    main()

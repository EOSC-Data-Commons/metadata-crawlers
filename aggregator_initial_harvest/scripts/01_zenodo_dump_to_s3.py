# Download latest Zenodo dump and push it to S3 bucket

# The Zenodo download takes ~13 GB on disk! The file is needed for streaming records
# to S3, after which it can be deleted. The download can last a few minutes.

# Uploading is done in parallel with multiple workers (--workers, default 16) -
# can take multiple hours locally to process all records!

# --bucket Mandatory! S3 bucket to upload the dump to.
# --local-file defines the filepath of the .tar.gz to download. Start at project root.
# --cleanup-local-dump can be set to "yes" to delete the downloaded .tar.gz after the script finishes


import argparse
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
    download_resumable,
    md5_of,
    upload,
)

logger = logging.getLogger(__name__)

# This file lives in <repo root>/aggregator_initial_harvest/scripts/
_REPO_ROOT = Path(__file__).resolve().parents[2]

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


def main():
    """Download the newest Zenodo dump and upload it to S3."""
    setup_logging()
    args = parse_args()

    logger.info("Start of script %s.", __file__)
    logger.debug(
        "Arguments: bucket=%s, local_file=%s, cleanup_local_dump=%s",
        args.bucket,
        args.local_file,
        args.cleanup_local_dump,
    )

    BUCKET = args.bucket
    PREFIX = "zenodo_dump/"

    # We'll first download the file locally, then stream it to S3. Otherwise direct streaming fails
    LOCAL_FILE = args.local_file

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

    resp = requests.get("https://zenodo.org/api/exporter", timeout=30, headers=HEADERS)
    resp.raise_for_status()

    # The JSON version of full metadata dump
    versions = resp.json()["records-json.tar.gz"]

    newest = max(versions, key=lambda v: datetime.fromisoformat(v["created"]))

    zenodo_metadata_url = newest["links"]["self"]  # URL to download the dump
    zenodo_metadata_date = newest["created"]

    # Expected hash for validation, as well as expectedfilesize
    EXPECTED_MD5 = newest["checksum"].removeprefix("md5:")
    EXPECTED_SIZE = int(newest["size"])

    logger.info(
        "Latest metadata dump: %s (created %s)",
        zenodo_metadata_url,
        zenodo_metadata_date,
    )
    logger.debug(
        "Expected size: %d bytes, expected md5: %s", EXPECTED_SIZE, EXPECTED_MD5
    )

    # Download file locally
    # Lengthy!
    logger.info("Downloading dump to %s", LOCAL_FILE)
    download_resumable(zenodo_metadata_url, LOCAL_FILE, EXPECTED_SIZE, HEADERS)
    logger.debug("Download finished, computing md5 of %s", LOCAL_FILE)
    actual_md5 = md5_of(LOCAL_FILE)

    # Checksum matches expected?
    # if corrupt file: remove it so the next run starts clean
    if actual_md5 != EXPECTED_MD5:
        logger.error(
            "Checksum mismatch: %s != %s. Removing %s",
            actual_md5,
            EXPECTED_MD5,
            LOCAL_FILE,
        )
        os.remove(LOCAL_FILE)
        raise RuntimeError(f"checksum mismatch: {actual_md5} != {EXPECTED_MD5}")
    logger.info("Download complete and verified.")

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

    # Optionally: if indicated, delete the local dump file to free up space
    if args.cleanup_local_dump == "yes":
        os.remove(LOCAL_FILE)
        logger.info("Deleted %s.", LOCAL_FILE)

    logger.info("Script %s complete.", __file__)


if __name__ == "__main__":
    main()

import hashlib
import logging
import os
import threading
import time

import boto3
import requests
from boto3.session import Session
from botocore.config import Config

logger = logging.getLogger(__name__)


class UploadStats:
    """
    A class to keep tally of successful and unsuccesful uploads.

    Worker threads call ``ok()`` and ``fail()``; the main thread reads the
    totals at the end of the run.

    Attributes:
        uploaded (int): Number of objects uploaded successfully.
        failed (int): Number of objects that failed to upload.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.uploaded = 0
        self.failed = 0

    def ok(self):
        """Count a record as successfully uploaded"""
        with self.lock:
            self.uploaded += 1
            n = self.uploaded
        if n % 10000 == 0:
            logger.info("%d files uploaded", n)

    def fail(self):
        """Count a record as failed to upload"""
        with self.lock:
            self.failed += 1


def connect_to_s3(
    access_key: str,
    secret_key: str,
    endpoint: str,
    max_pool_connections: int = 10,
) -> boto3.client:
    """Connect to S3, return a client."""
    session = Session()
    return session.client(
        service_name="s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=str(endpoint),
        # Don't add checksums unless required,
        # Leaving this out results in errors uploading .tar records
        config=Config(
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
            max_pool_connections=max_pool_connections,
        ),
    )


def read_object(s3, bucket, key):
    """Read one S3 object and return its contents as UTF-8 text.

    Args:
        s3: boto3 S3 client.
        bucket (str): Bucket name.
        key (str): Object key.

    Returns:
        str: The decoded object contents.
    """
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")


def download_resumable(
    url: str,
    path: str,
    total: int,
    headers: dict[str, str],
    chunk: int = 4 * 1024 * 1024,
) -> None:
    """Download a file to disk, resuming after connection errors.

    Streams ``url`` into ``path``. If the connection drops or a request fails,
    waits with exponential backoff (up to 60 s) and continues from the bytes
    already on disk using an HTTP ``Range`` request.

    If ``path`` already holds ``total`` bytes or more, returns immediately.

    Args:
        url (str): Address of the file to download.
        path (str): Local file path; created if missing, appended to if it
            already exists from an earlier partial download.
        total (int): Expected final size in bytes (e.g. the ``size`` field from
            the Zenodo exporter API). The loop ends once the file reaches this size.
        headers (dict): Request headers. A ``Range`` header is added automatically on
            resume and must not be included here.
        chunk (int): Read size in bytes per iteration. Defaults to 4 MiB.

    Returns:
        None
    """
    attempt = 0
    while True:
        have = os.path.getsize(path) if os.path.exists(path) else 0
        if have >= total:
            return
        req_headers = dict(headers)
        if have:
            req_headers["Range"] = f"bytes={have}-"
        try:
            with requests.get(
                url, headers=req_headers, stream=True, timeout=(10, 120)
            ) as r:
                r.raise_for_status()
                if have and r.status_code != 206:
                    logger.warning("server ignored Range; restarting from zero")
                    have = 0
                with open(path, "ab" if have else "wb") as f:
                    # raw stream, no decoding: the file is already a .gz and must stay byte-identical
                    for block in r.raw.stream(chunk, decode_content=False):
                        f.write(block)
                        have += len(block)
                        if have % (256 * 1024 * 1024) < chunk:
                            logger.info("%.2f / %.2f GB", have / 1e9, total / 1e9)
        except (requests.exceptions.RequestException, OSError) as e:
            attempt += 1
            now = os.path.getsize(path) if os.path.exists(path) else 0
            logger.warning(
                "interrupted at %.0f MB (retry %d): %s",
                now / 1e6,
                attempt,
                type(e).__name__,
            )
            time.sleep(min(60, 2 ** min(attempt, 6)))
            continue
        attempt = 0  # a clean pass resets the counter; loop re-checks size


def md5_of(path):
    """
    Get md5 hash of file at ``path``.
    Returns: str
    """
    h = hashlib.md5()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def download_verified(url, path, expected_md5, expected_size, headers):
    """
    Download a file (resuming after drops) and verify its md5.

    A failed checksum means a corrupt download: the file is removed so
    the next run starts clean, and a RuntimeError is raised.

    Args:
        url (str): Address of the file to download.
        path (str): Local file path to download into.
        expected_md5 (str): md5 the file must have, from the exporter API.
        expected_size (int): Expected final size in bytes.
        headers (dict): Request headers (User-Agent).

    Returns:
        None
    """
    download_resumable(url, path, expected_size, headers)
    logger.debug("Download finished, computing md5 of %s", path)
    actual_md5 = md5_of(path)

    # Checksum matches expected?
    # if corrupt file: remove it so the next run starts clean
    if actual_md5 != expected_md5:
        logger.error(
            "Checksum mismatch: %s != %s. Removing %s",
            actual_md5,
            expected_md5,
            path,
        )
        os.remove(path)
        raise RuntimeError(f"checksum mismatch: {actual_md5} != {expected_md5}")
    logger.info("Download of %s complete and verified.", path)


def upload(
    s3_client: boto3.client,
    bucket: str,
    key: str,
    data: bytes,
    stats: UploadStats,
) -> None:
    """Upload one object to S3 and record the outcome.

    Can be run in a worker thread. It never raises exceptions: failures are
    logged with the full error message and traceback and counted on the
    ``stats`` object.

    Args:
        s3_client: boto3 S3 client, shared across workers.
        bucket (str): Name of the destination bucket.
        key (str): Object key to write, e.g. ``"zenodo_dump/1003105.json"``.
        data (bytes): Full object content read into memory.
        stats (UploadStats): Results tally ; ``ok()`` is called on
            success and ``fail()`` on any exception.

    Returns:
        None
    """
    try:
        s3_client.put_object(Bucket=bucket, Key=key, Body=data)
        stats.ok()
    except Exception as e:
        logger.exception("failed to upload %s: %s", key, e)
        stats.fail()


def iter_s3_keys(s3, bucket, prefix, suffix=".json", start_after=""):
    """Lazily yield object keys under bucket/prefix, in alphabetical order."""
    kwargs = {"Bucket": bucket, "Prefix": prefix}
    if start_after:
        kwargs["StartAfter"] = start_after
    for page in s3.get_paginator("list_objects_v2").paginate(**kwargs):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(suffix):
                yield obj["Key"]
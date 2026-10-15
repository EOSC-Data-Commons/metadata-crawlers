from __future__ import annotations

import logging
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 120
RETRY_STATUS = {429, 500, 502, 503, 504}



def make_client(timeout: float = DEFAULT_TIMEOUT) -> httpx.Client:
    """Create an httpx client. Each adapter keeps its own module-level client."""
    return httpx.Client(timeout = httpx.Timeout(timeout))



def get_json(
    client: httpx.Client, url: str, *, params: dict[str, Any] | None = None,
    attempts: int = 3, backoff: float = 2.0, **kwargs: Any) -> Any:
    """
    GET `url` and return the parsed JSON, retrying on transport errors and on
    429/5xx responses (waits `backoff`, `2 * backoff`, ... seconds in between).

    Any other HTTP error (e.g. 404) is raised immediately via `raise_for_status`.
    After the last attempt the original exception is raised.
    """
    for attempt in range(1, attempts + 1):
        try:
            response = client.get(url, params = params, **kwargs)
            if response.status_code in RETRY_STATUS and attempt < attempts:
                raise httpx.HTTPStatusError(
                    f"retryable status {response.status_code}",
                    request = response.request,
                    response = response,
                )
            response.raise_for_status()
            return response.json()
        except (httpx.TransportError, httpx.HTTPStatusError) as e:
            retryable = isinstance(e, httpx.TransportError) or (
                e.response.status_code in RETRY_STATUS
            )
            if not retryable or attempt == attempts:
                raise
            delay = backoff * 2 ** (attempt - 1)
            logger.warning(
                "GET %s failed (%s), retrying in %.0fs (attempt %d/%d)",
                url, e, delay, attempt, attempts,
            )
            time.sleep(delay)
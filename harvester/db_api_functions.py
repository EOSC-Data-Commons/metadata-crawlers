import httpx
import logging
from typing import Optional
from harvester.settings import settings
from harvester.models import HarvestRunCreateResponse

logger = logging.getLogger(__name__)

timeout = settings.WAREHOUSE_API_TIMEOUT
base_url = settings.WAREHOUSE_API_URL
# warehouse API routes:
HARVEST_RUN_URL = f"{base_url}/harvest_run"
HARVEST_EVENT_URL = f"{base_url}/harvest_event"

# shared HTTP client for warehouse API
_WAREHOUSE_CLIENT = httpx.Client(timeout=timeout)


def start_harvest_run(harvest_url: str) -> Optional[HarvestRunCreateResponse]:
    """
    POST /harvest_run to create a new harvest run.

    :param harvest_url: endpoint for harvesting
    :return: HarvestRun object; returns None on error.
    """
    payload = {"harvest_url": harvest_url}
    try:
        response = _WAREHOUSE_CLIENT.post(HARVEST_RUN_URL, json=payload)
        response.raise_for_status()
        run = HarvestRunCreateResponse(**response.json())
        logger.info("Started harvest run id=%s.", run.id)
        return run
    except httpx.RequestError as e:
        logger.error("Failed to start harvest run for %s: %s", harvest_url, e)
        return None

def get_open_run_id(harvest_url: str) -> Optional[str]:
    """
    GET /harvest_run to fetch an open harvest run ID if it exists.

    :param harvest_url: endpoint for harvesting
    :return: run ID if status is 'open', otherwise None
    """
    params = {"harvest_url": harvest_url}
    try:
        response = _WAREHOUSE_CLIENT.get(HARVEST_RUN_URL, params=params)
        response.raise_for_status()

        response = response.json()
        runs = response.get("harvest_runs", [])

        if not runs:
            return None

        run = runs[0]

        if run.get("status") == "open":
            return run.get("id")

        return None

    except httpx.HTTPStatusError as e:
        logger.error("HTTP error while checking open harvest run for %s: %s", harvest_url, e.response.text)
        return None

    except httpx.RequestError as e:
        logger.error("Network error while checking open harvest run for %s: %s", harvest_url, e)
        return None


def close_harvest_run(payload: dict) -> None:
    """
    PUT /harvest_run to close the harvest run.

    :param payload: payload for API post request to close the harvest run
    """
    run_id = payload.get("id")
    try:
        response = _WAREHOUSE_CLIENT.put(HARVEST_RUN_URL, json=payload)
        response.raise_for_status()
        logger.info(
            "Closed harvest run %s — started %s, finished %s",
            run_id,
            payload.get("started_at"),
            payload.get("completed_at"),
        )
    except httpx.RequestError as e:
        logger.error("Failed to close harvest run %s: %s", run_id, e)


def send_harvest_event(event_payload: dict) -> bool:
    """
    Send event_payload to API.

    :param event_payload: dictionary containing event data for harvest_event route
    :return logical: True if the payload has been sent to API successfully
    """
    try:
        response = _WAREHOUSE_CLIENT.post(HARVEST_EVENT_URL, json=event_payload)
        response.raise_for_status()
        return True
    except httpx.HTTPStatusError as e:
        logger.error("Failed to send record %s to API: HTTP status error %s: %s", event_payload.get("record_identifier"), e, e.response.text)
        return False
    except httpx.RequestError as e:
        logger.error("Failed to send record %s to API: Request error %s", event_payload.get("record_identifier"), e)
        return False


def close_warehouse_client():
    try:
        _WAREHOUSE_CLIENT.close()
    except Exception:
        logger.warning("Failed to close warehouse client")
        pass
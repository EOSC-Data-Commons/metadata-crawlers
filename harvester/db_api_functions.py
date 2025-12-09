import os
import requests
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

WAREHOUSE_API_URL = os.getenv("WAREHOUSE_API_URL")
# warehouse API routes:
HARVEST_RUN_URL = f"{WAREHOUSE_API_URL}/harvest_run"
HARVEST_EVENT_URL = f"{WAREHOUSE_API_URL}/harvest_event"


def start_harvest_run(harvest_url: str, timeout: int = 30) -> Optional[Dict[str, Any]]:
    """
    POST /harvest_run to create a new harvest run. 
    
    :param harvest_url: endpoint for harvesting
    :param timeout: Request timeout in seconds
    :return: JSON response (dict) containing 'harvest_run_id', optionally 'last_harvest_date', and endpoint config; returns None on error.
    """
    payload = {"harvest_url": harvest_url}
    try:
        response = requests.post(HARVEST_RUN_URL, json=payload, timeout=timeout)
        response.raise_for_status()
        run_info = response.json()
        logger.info("Started harvest run id=%s.", run_info.get("id"))
        return run_info
    except requests.RequestException as e:
        logger.error("Failed to start harvest run for %s: %s", harvest_url, e)
        return None

def get_open_run_id(harvest_url: str, timeout: int = 30) -> Optional[Dict]:
    """
    GET /harvest_run to fetch an open harvest run ID if it exists.

    :param harvest_url: endpoint for harvesting
    :param timeout: Request timeout in seconds
    :return: JSON response (dict) containing the ID of a harvest run and its status, or None if not found or failed
    """
    params = {"harvest_url": harvest_url}
    try:
        response = requests.get(HARVEST_RUN_URL, params=params, timeout=timeout)
        response.raise_for_status()

        response = response.json()
        if response and response.get("status") == "open":
            return response.get("id")
        else:
            return None

    except requests.RequestException as e:
        logger.error("Error checking for open harvest run for %s: %s", harvest_url, e)
        return None

def close_harvest_run(payload: Dict) -> None:
    """
    PUT /harvest_run to close the harvest run.

    :param payload: payload for API post request to close the harvest run
    """
    run_id = payload.get("id")
    try:
        response = requests.put(HARVEST_RUN_URL, json=payload, timeout=30)
        response.raise_for_status()
        logger.info(
            "Closed harvest run %s — started %s, finished %s",
            run_id,
            payload.get("started_at"),
            payload.get("completed_at"),
        )
    except requests.RequestException as e:
        if e.response is not None:
            logger.error("Failed to close harvest run %s: API error %s %s", run_id, e.response.status_code, e.response.text)
        else:
            logger.error("Failed to close harvest run %s: %s", run_id, e)


def send_harvest_event(event_payload: Dict) -> bool:
    """
    Send event_payload to API.

    :param event_payload: dictionary containing event data for harvest_event route
    :return logical: True if the payload has been sent to API successfully 
    """
    try:
        response = requests.post(HARVEST_EVENT_URL, json=event_payload, timeout=60)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        logger.error("Failed to send record %s to API: %s", event_payload.get("record_identifier"), e)
        return False

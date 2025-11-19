import argparse
import logging
import os
from datetime import datetime, timezone

from .harvester_oaipmh import run_harvester_oaipmh
from .api_client import start_harvest_run, close_harvest_run, get_open_run_id


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

API_BASE_URL = os.getenv("API_BASE_URL")

def main():
    parser = argparse.ArgumentParser(description="Metadata Harvester")
    parser.add_argument("harvest_url", help="Repository harvesting endpoint")
    args = parser.parse_args()

    harvest_url = args.harvest_url

    # start a new harvest run
    start_time = datetime.now(timezone.utc).isoformat(timespec='seconds')

    try:
        run_info = start_harvest_run(harvest_url)
        # if there is no response, try closing it
        if run_info is None:
            logger.error("Failed to start harvest run, checking for existing open run...")
            open_run_id = get_open_run_id(harvest_url)  
            if open_run_id:
                logger.warning("Closing existing open harvest run %s as failed", open_run_id)
                end_time = datetime.now(timezone.utc).isoformat(timespec='seconds')
                close_harvest_run_payload = {
                    "id": open_run_id,
                    "success": False,
                    "started_at": start_time,
                    "completed_at": end_time
                    }
                close_harvest_run(API_BASE_URL, close_harvest_run_payload)
            return 1

        harvest_run_id = run_info["id"]
        config = run_info.get("endpoint_config")
        if not config:
            raise ValueError("Missing endpoint_config in API response")
    
        harvesting_protocol = config.get("protocol")

        if harvesting_protocol == "OAI-PMH":
            harvest_success = run_harvester_oaipmh(run_info)
        else:
            raise ValueError(f"Unsupported protocol: {harvesting_protocol}")


    except Exception as e:
        logger.exception("Harvest encountered an error: %s", e)
        harvest_success = False

    
    finally:
        end_time = datetime.now(timezone.utc).isoformat(timespec='seconds')
        close_harvest_run_payload = {
            "id": harvest_run_id,
            "success": harvest_success,
            "started_at": start_time,
            "completed_at": end_time,
        }
        close_harvest_run(API_BASE_URL, close_harvest_run_payload)

    return 0 if harvest_success else 1
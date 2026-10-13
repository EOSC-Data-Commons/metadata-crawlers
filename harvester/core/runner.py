from __future__ import annotations

import json
import logging
from typing import Any

from ..db_api_functions import send_harvest_event
from .base import SourceAdapter
from .datacite import to_datacite_oai_xml

logger = logging.getLogger(__name__)


def _log_summary(record_count: int, harvest_events: int, failed_events: int) -> None:
    logger.info(
        "Harvest summary: processed %s records, successfully sent %s of them to the warehouse, failed to send %s records.",
        record_count,
        harvest_events,
        failed_events,
    )


def run_harvester(run_info: dict[str, Any], adapter: SourceAdapter) -> bool:
    """
    Run a full (or incremental) harvest for any `SourceAdapter` and push each
    record to the data warehouse as a harvest event.

    :param run_info: Dictionary describing the harvest run. Uses
            `endpoint_config` (with `harvest_url` and `code`), `from_date`
            (None = full harvest), `until_date` (required) and `id`.
    :param adapter: The repository-specific adapter.

    :return: `True` if every harvest event was built and sent successfully
            (and no unexpected exception occurred), `False` otherwise.
    """
    record_count = 0
    harvest_events = 0
    failed_events = 0
    try:
        config = run_info.get("endpoint_config")
        if config is None:
            raise ValueError("config is missing")
        harvest_url = config.get("harvest_url")
        from_date = run_info.get("from_date")
        until_date = run_info.get("until_date")
        if until_date is None:
            raise ValueError("Missing until_date parameter")

        for raw in adapter.iter_raw(from_date, until_date):
            record_count += 1
            try:
                record = adapter.build_record(raw)
                event_payload = {
                    "record_identifier": record.record_id,
                    "datestamp": record.datestamp,
                    "raw_metadata": to_datacite_oai_xml(record),
                    "additional_metadata": json.dumps(record.additional_metadata),
                    "harvest_url": harvest_url,
                    "repo_code": config.get("code"),
                    "harvest_run_id": run_info.get("id"),
                    "is_deleted": record.is_deleted,
                }
            except Exception:
                logger.exception("[%s] Failed to build record from %.200r", adapter.name, raw)
                failed_events += 1
                continue

            if send_harvest_event(event_payload):
                harvest_events += 1
            else:
                failed_events += 1

        _log_summary(record_count, harvest_events, failed_events)
        return failed_events == 0

    except Exception as e:
        logger.exception("[%s] Unexpected error in harvest run: %s", adapter.name, e)
        _log_summary(record_count, harvest_events, failed_events)
        return False
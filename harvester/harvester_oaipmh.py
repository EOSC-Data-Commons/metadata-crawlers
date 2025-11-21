import json
import time
import logging
import traceback
import os
import requests
from datetime import datetime, timezone
from lxml import etree as ET
from oaipmh_scythe import Scythe
from typing import Dict, Optional, Any

from .db_api_functions import send_harvest_event

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
API_BASE_URL = os.getenv("API_BASE_URL")


def fetch_dataverse_json(doi: str, base_url: str, exporter: str) -> Optional[str]:
    """
    Fetch additional metadata: dataverse json

    :param doi: record identifier
    :param base_url: dataverse API endpoint
    :param exporter: exporter type
    :return: stringified JSON with additional metadata; returns None on error
    """
    params = {"exporter": exporter, "persistentId": doi}
    try:
        response = requests.get(base_url, params=params, timeout=30)
        if response.status_code == 200:
            return json.dumps(response.json(), indent=2)
        else:
            logger.warning("Failed to fetch Dataverse JSON for %s: %s", doi, response.status_code)
            return None
    except Exception as e:
        logger.exception("Dataverse JSON fetch error for %s", doi)
        return None


def fetch_additional_oai(record_id: str, base_url: str, metadata_prefix: str) -> Optional[str]:
    """
    Fetch additional metadata: OAI-PMH

    :param record_id: OAI-PMH record identifier
    :param base_url: OAI-PMH endpoint
    :param metadata_prefix: metadata format
    :return: stringified XML with additional metadata; returns None on error
    """
    try:
        with Scythe(base_url) as client:
            record = client.get_record(identifier=record_id, metadata_prefix=metadata_prefix)
            return ET.tostring(record.xml, pretty_print=True, encoding="unicode")
    except Exception as e:
        logger.warning("Error fetching %s metadata for %s: %s", metadata_prefix, record_id, e)
        return None


def apply_xslt_transform(ddi_xml: str, transform: ET.XSLT) -> str | None:
    """
    Apply a precompiled XSLT transform to a DDI XML string. (for SwissUBase)

    :param ddi_xml: DDI XML as string
    :param transform: Compiled lxml.etree.XSLT object
    :return: Transformed XML as string, or None on failure
    """
    try:
        ddi_doc = ET.fromstring(ddi_xml.encode("utf-8"))
        result_tree = transform(ddi_doc)
        return ET.tostring(result_tree, pretty_print=True, encoding="UTF-8").decode("utf-8")
    except Exception as e:
        logger.warning("Transformation failed: %s", e)
        return None
    

def run_harvester_oaipmh(run_info: dict) -> bool:
    """
    Run an OAI-PMH harvest.

    :param run_info (dict): info about the harvest run including: 
        harvest_run_id, 
        from and until dates,
        endpoint_config

    :return bool: True if harvest succeeded, False otherwise
    """

    try:
        # extract run info and harvest params
        harvest_run_id = run_info.get("id")
        from_date = run_info.get("from_date")
        from_ = datetime.strptime(from_date, '%Y-%m-%dT%H:%M:%S.%f%z').strftime('%Y-%m-%dT%H:%M:%SZ') if from_date else None
        until_date = run_info.get("until_date")
        until = datetime.strptime(until_date, '%Y-%m-%dT%H:%M:%S.%f%z').strftime('%Y-%m-%dT%H:%M:%SZ')

        config = run_info.get("endpoint_config")
        harvest_url = config.get("harvest_url")
        harvest_params = config.get("harvest_params")
        metadata_prefix = harvest_params.get("metadata_prefix", "oai_dc")
        set_ = harvest_params.get("set")
        code = config.get("code")
        additional = harvest_params.get("additional_metadata_params")
        additional_protocol = additional.get("protocol") if additional else None

        # if schema is not DataCite, we will need to transform the XML
        if metadata_prefix == "oai_ddi25":
            XSLT_PATH = os.path.join(BASE_DIR, "ddi_to_datacite.xsl")
            xslt_doc = ET.parse(XSLT_PATH)
            transform = ET.XSLT(xslt_doc)

        # harvesting
        with Scythe(harvest_url, timeout=180, max_retries=3, default_retry_after=60) as client:
            if from_:
                logger.info("Incremental harvest since %s", from_date)
                records = client.list_records(
                    from_=from_,
                    until=until,
                    metadata_prefix=metadata_prefix,
                    set_=set_
                )
            else:
                logger.info("First harvest, fetching all records.")
                records = client.list_records(
                    until=until,
                    metadata_prefix=metadata_prefix,
                    set_=set_,
                    ignore_deleted=True
                )

            record_count = 0
            harvest_events = 0
            failed_events = 0

            for record in records:
                record_count += 1

                try:
                    identifier = record.header.identifier
                    datestamp = record.header.datestamp
                    is_deleted = getattr(record.header, "status", None) == "deleted"
                    raw_metadata = ET.tostring(record.xml, pretty_print=True, encoding="unicode")

                    additional_metadata = None

                    if not is_deleted:
                        if additional_protocol == "REST_API":
                            additional_metadata = fetch_dataverse_json(
                                doi=identifier,
                                base_url=additional["endpoint"],
                                exporter=additional["format"]
                            )

                        elif additional_protocol == "OAI-PMH":
                            additional_metadata = fetch_additional_oai(
                                record_id=identifier,
                                base_url=additional["endpoint"],
                                metadata_prefix=additional["format"]
                            )

                        elif metadata_prefix == "oai_ddi25":
                            additional_metadata = raw_metadata
                            raw_metadata = apply_xslt_transform(raw_metadata, transform)
                            if raw_metadata is None:
                                logger.warning("Skipping record %s: transformation to DataCite failed.", identifier)
                                failed_events += 1
                                continue

                    # metadata and record info to be sent to the warehouse
                    event_payload = {
                        "record_identifier": identifier,
                        "datestamp": datestamp,
                        "raw_metadata": raw_metadata,
                        "additional_metadata": additional_metadata,
                        "harvest_url": harvest_url,
                        "repo_code": code,                          # do we need this?
                        "harvest_run_id": harvest_run_id,
                        "is_deleted": is_deleted
                    }
                    
                    if send_harvest_event(API_BASE_URL, event_payload):
                        harvest_events += 1
                    else:
                        failed_events += 1

                except Exception as e:
                    failed_events += 1
                    print(f"Record %s failed: %s", record_count, e)

                if record_count % 100 == 0:
                    time.sleep(1)

        logger.info(
            "Harvest summary: processed %s records, successfully sent %s of them to the warehouse, failed to send %s records.",
            record_count,
            harvest_events,
            failed_events
        )

        if failed_events == 0:
            return True
        else:
            return False

    except Exception:
        logger.exception("Unexpected error in run_harvester_oaipmh")
        logger.info(
            "Harvest summary: processed %s records, successfully sent %s of them to the warehouse, failed to send %s records.",
            record_count,
            harvest_events,
            failed_events
        )
        return False
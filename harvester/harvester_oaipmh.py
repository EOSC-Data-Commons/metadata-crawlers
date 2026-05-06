import json
import logging
import os
import httpx
import time
from datetime import datetime
from lxml import etree as ET
from oaipmh_scythe import Scythe
from typing import Optional

from .db_api_functions import send_harvest_event

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# shared http client for Dataverse requests
_DATAVERSE_CLIENT = httpx.Client(timeout=30)


def close_dataverse_client():
    try:
        _DATAVERSE_CLIENT.close()
    except Exception:
        logger.warning("Failed to close Dataverse client")
        pass

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
        response = _DATAVERSE_CLIENT.get(base_url, params=params)
        response.raise_for_status()
        return json.dumps(response.json(), indent=2)
    except httpx.HTTPStatusError as e:
        logger.warning(
            "Failed to fetch Dataverse JSON for %s: HTTP %s",
            doi,
            e.response.status_code if e.response else "N/A",
        )
        return None
    except httpx.RequestError as e:
        logger.error("Network error fetching Dataverse JSON for %s: %s", doi, e)
        return None

def fetch_additional_metadata_hal(record_id: str, base_url: str) -> Optional[str]:
    """
    Fetch file metadata from the HAL Search API for a given HAL record.
    
    Args:
        record_id (str): HAL identifier.
        base_url (str): HAL Search API endpoint.

    Returns:
        Optional[str]: JSON response from the API, or None if
                       the record was not found or the request failed.
    """

    # Remove version suffix from the ID because query doesn't accept version suffix
    hal_id_without_version = record_id.split("v")[0]
    params = {
        "q": f"halId_s:{hal_id_without_version}",
        "wt": "json",
        # Request only the fields needed to locate and describe attached files
        "fl": ",".join([
            "halId_s",         # document identifier
            "fileMain_s",      # URL of the primary attached file
            "files_s",         # URLs of all attached files
            "fileType_s",      # file type (e.g. PDF)
            "modifiedDate_tdate",   # last modification date
            "producedDate_tdate",   # production/publication date
            "version_i",       # version number
        ]),
    }

    try:
        response = _DATAVERSE_CLIENT.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        if not data.get("response", {}).get("docs"):
            logger.warning("No HAL records found for %s", record_id)
            return None
        return json.dumps(data, indent=2)

    except httpx.HTTPStatusError as e:
        logger.warning(
            "Failed to fetch HAL JSON for %s: HTTP %s",
            record_id,
            e.response.status_code if e.response else "N/A",
        )
        return None

    except httpx.RequestError as e:
        logger.error(
            "Network error fetching HAL JSON for %s: %s",
            record_id,
            e,
        )
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
    
def transformation_and_additional_metadata(raw_metadata, metadata_prefix, identifier, additional_protocol, additional_endpoint, additional_format):
    """
    Transform metadata into DataCite format and optionally fetch additional metadata.

    This function performs two main tasks:
    1. If the input metadata is not already in a DataCite-compatible format, it applies an XSLT transformation to convert it.
    2. Depending on configuration, it may fetch additional metadata from external services (e.g., Dataverse API, OAI-PMH, HAL API).

    Args:
        raw_metadata (lxml.etree._Element): The original metadata record (typically XML).
        metadata_prefix (str): The metadata format identifier (e.g., "oai_dc", "oai_ddi25", "datacite").
        identifier (str): Unique identifier of the record (e.g., DOI or OAI identifier).
        additional_protocol (str): Name of protocol that is used for additional metadata (OAI-PMH, DATAVERSE_API, HAL_API...)
        additional_endpoint (str): Base endpoint URL for additional metadata
        additional_format (str): Additional parameter that is needed for some endpoints

    Returns (raw_metadata, additional_metadata) or None where:
        - raw_metadata is the transformed (or original) metadata
        - additional_metadata is either:
            * original metadata (if transformed), or
            * fetched metadata from an external service
        Returns None if transformation fails.
    """

    # if schema is not DataCite, we will need to transform the XML
    if metadata_prefix == "oai_ddi25":
        XSLT_PATH = os.path.join(BASE_DIR, "ddi_to_datacite.xsl")
        xslt_doc = ET.parse(XSLT_PATH)
        transform = ET.XSLT(xslt_doc)

    if metadata_prefix == "oai_dc":
        XSLT_PATH = os.path.join(BASE_DIR, "dc_to_datacite.xsl")
        xslt_doc = ET.parse(XSLT_PATH)
        transform = ET.XSLT(xslt_doc)

    additional_metadata = None

    try:
        if metadata_prefix not in ["oai_datacite", "oai_datacite4", "datacite"]: # if metadata_prefix is not in datacite format
            additional_metadata = raw_metadata
            raw_metadata = apply_xslt_transform(raw_metadata, transform)
            if raw_metadata is None:
                logger.warning("Skipping record %s: transformation to DataCite failed.", identifier)
                return None, None

        elif additional_protocol == "DATAVERSE_API": # DANS
            additional_metadata = fetch_dataverse_json(
                doi=identifier,
                base_url=additional_endpoint,
                exporter=additional_format
            )

        elif additional_protocol == "OAI-PMH": # DABAR
            additional_metadata = fetch_additional_oai(
                record_id=identifier,
                base_url=additional_endpoint,
                metadata_prefix=additional_format
            )

        elif additional_protocol == "HAL_API": # HAL
            identifier_for_additional_metadata = identifier.split(":")[-1]
            additional_metadata = fetch_additional_metadata_hal(
                record_id=identifier_for_additional_metadata,
                base_url=additional_endpoint
            )

    except Exception as e:
        logger.error("Error when fetching additional metadata: %s", e)
        return None, None
    
    return (raw_metadata, additional_metadata)
    

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
        record_count = 0
        harvest_events = 0
        failed_events = 0
        
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
        sets = harvest_params.get("set") if harvest_params.get("set") else [None]
        code = config.get("code")
        
        # here we define for which repositories we need to add timeout in order get back all the records from them
        repository_name = config.get("name")
        need_timeout = False
        if repository_name in ["ALBA", "Riga Stradins University", "CLARIN-IV", "ZENODO"]:
            need_timeout = True

        # harvesting
        with Scythe(harvest_url, timeout=180, max_retries=3, default_retry_after=60) as client:

            # if we have more than one set then we iterate in for loop for each set
            for set_name in sets:
                if from_:
                    logger.info("Incremental harvest since %s", from_date)
                    records = client.list_records(
                        from_=from_,
                        until=until,
                        metadata_prefix=metadata_prefix,
                        set_=set_name
                    )
                else:
                    logger.info("First harvest, fetching all records.")
                    records = client.list_records(
                        #until=until, # some PaNOSC repositories won't work with "until" parameter so we do not need to pass this parameter at all
                        metadata_prefix=metadata_prefix,
                        set_=set_name,
                        ignore_deleted=True
                    )

                for record in records:
                    record_count += 1

                    # after every 10 records add 1 second sleep
                    if need_timeout:
                        if record_count % 10 == 0:
                            time.sleep(2)

                    try:
                        identifier = record.header.identifier
                        datestamp = record.header.datestamp
                        is_deleted = getattr(record.header, "status", None) == "deleted"
                        raw_metadata = ET.tostring(record.xml, pretty_print=True, encoding="unicode")
                        
                        # special case where we skip some records for PaNOSC ALBA repository because those records have poor metadata
                        if repository_name == "ALBA":
                            setSpecs = record.header.setSpecs
                            if setSpecs == []:
                                continue

                        harvest_params = config.get("harvest_params")
                        additional = harvest_params.get("additional_metadata_params")
                        additional_protocol = additional.get("protocol") if additional else None
                        additional_endpoint = additional["endpoint"] if additional else None
                        additional_format = additional["format"] if additional else None

                        # Identifier for additional metadata without namespace (everything after last ":")
                        identifier_for_additional_metadata = identifier.split(":")[-1]
                        additional_metadata = None

                        if not is_deleted:
                            raw_metadata, additional_metadata = transformation_and_additional_metadata(raw_metadata, 
                                                                                                       metadata_prefix, 
                                                                                                       identifier,
                                                                                                       additional_protocol,
                                                                                                       additional_endpoint,
                                                                                                       additional_format)
                            if raw_metadata is None:
                                failed_events += 1
                                continue

                        # metadata and record info to be sent to the warehouse
                        event_payload = {
                            "record_identifier": identifier_for_additional_metadata if code != 'FinBIF' else identifier,
                            "datestamp": datestamp,
                            "raw_metadata": raw_metadata,
                            "additional_metadata": additional_metadata,
                            "harvest_url": harvest_url,
                            "repo_code": code,
                            "harvest_run_id": harvest_run_id,
                            "is_deleted": is_deleted
                        }
                        
                        if send_harvest_event(event_payload):
                            harvest_events += 1
                        else:
                            failed_events += 1

                    except Exception as e:
                        failed_events += 1
                        logger.error("Record %s failed: %s", record_count, e)


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
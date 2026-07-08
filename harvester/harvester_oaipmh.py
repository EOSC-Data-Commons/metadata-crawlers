import json, logging, os, httpx, time, requests

from urllib.parse import urlparse
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
    
    :param record_id: HAL record identifier
    :param base_url: HAL Search API endpoint
    :return: stringified JSON response with additional metadata;
            returns None if the request fails or the record is not found
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


def apply_xslt_transform(xml: str, transform: ET.XSLT) -> str | None:
    """
    Apply a precompiled XSLT transform to a XML string.

    :param xml: XML as string
    :param transform: Compiled lxml.etree.XSLT object
    :return: Transformed XML as string, or None on failure
    """
    try:
        doc = ET.fromstring(xml.encode("utf-8"))
        result_tree = transform(doc)
        return ET.tostring(result_tree, pretty_print=True, encoding="UTF-8").decode("utf-8")
    except Exception as e:
        logger.warning("Transformation failed: %s", e)
        return None
    
def transformation_and_additional_metadata(raw_metadata: str, 
                                           metadata_prefix: str, 
                                           identifier: str, 
                                           additional_protocol: str | None, 
                                           additional_endpoint: str | None, 
                                           additional_format: str | None) -> tuple[str, str]:
    """
    Transform metadata into DataCite format and optionally fetch additional metadata.

    This function performs two main tasks:
    1. If the original format is not DataCite already and a transformation has to be applied, 
    the original XML is stored as additional metadata, like in the case of DABAR
    2. Depending on configuration, it may fetch additional metadata from external services (e.g., Dataverse API, OAI-PMH, HAL API).

    :param raw_metadata (str): The original metadata record (typically XML).
    :param metadata_prefix (str): The metadata format identifier (e.g., "oai_dc", "oai_ddi25", "datacite").
    :param identifier (str): Unique identifier of the record (e.g., DOI or OAI identifier).
    :param additional_protocol (str | None): Name of protocol that is used for additional metadata (OAI-PMH, DATAVERSE_API, HAL_API...)
    :param additional_endpoint (str | None): Base endpoint URL for additional metadata
    :param additional_format (str | None): Additional parameter that is needed for some endpoints

    :return: tuple of (raw_metadata, additional_metadata), where raw_metadata is the
            transformed (or original) metadata, and additional_metadata is
            either the original metadata, externally fetched metadata, or None.
            Returns (None, None) on failure.
    """

    # if schema is not DataCite, we will need to transform the XML
    additional_metadata = None

    try:
        if metadata_prefix not in ["oai_datacite", "oai_datacite4", "datacite"]: # if metadata_prefix is not in datacite format
            transform = None
            if metadata_prefix == "oai_ddi25":
                XSLT_PATH = os.path.join(BASE_DIR, "ddi_to_datacite.xsl")
                xslt_doc = ET.parse(XSLT_PATH)
                transform = ET.XSLT(xslt_doc)

            if metadata_prefix == "oai_dc":
                XSLT_PATH = os.path.join(BASE_DIR, "dc_to_datacite.xsl")
                xslt_doc = ET.parse(XSLT_PATH)
                transform = ET.XSLT(xslt_doc)
                
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



def resolve_zenodo_identifier(doi: str) -> str | None:
    """
    Resolve a Zenodo DOI (e.g. "10.5281/zenodo.1170128") by following the
    DOI redirect to the actual Zenodo record page and extracting the record ID.

    :param doi: DOI string, e.g. "10.5281/zenodo.1170128"
    :return: the resolved Zenodo record ID as a string, or None if it couldn't be resolved
    """
    doi_url = f"https://doi.org/{doi}"

    try:
        response = requests.get(doi_url, allow_redirects = True)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error("Failed to resolve DOI %s: %s", doi, e)
        return None

    resolved_url = response.url

    # Zenodo record URLs look like https://zenodo.org/records/1170128 or /record/1170128
    path = urlparse(resolved_url).path.rstrip("/")  # e.g. "/records/1170128"
    record_id = path.rsplit("/", 1)[-1]              # "1170128"

    if not record_id.isdigit():
        logger.warning("Could not extract Zenodo record id from resolved URL %s (DOI: %s)", resolved_url, doi)
        return None

    return f"oai:zenodo.org:{record_id}"



def process_record(record, config, metadata_prefix, harvest_url, code, harvest_run_id, repository_name):
    """
    Process a single OAI-PMH record and send it to the warehouse.
    :return str: "sent", "failed", or "skipped"
    """
    identifier = record.header.identifier
    datestamp = record.header.datestamp
    is_deleted = getattr(record.header, "status", None) == "deleted"
    raw_metadata = ET.tostring(record.xml, pretty_print=True, encoding="unicode")

    # special case where we skip some records for PaNOSC ALBA repository because those records have poor metadata
    if repository_name == "ALBA":
        setSpecs = record.header.setSpecs
        if setSpecs == []:
            return "skipped"

    harvest_params = config.get("harvest_params")
    additional = harvest_params.get("additional_metadata_params")
    additional_protocol = additional.get("protocol") if additional else None
    additional_endpoint = additional["endpoint"] if additional else None
    additional_format = additional["format"] if additional else None

    # Identifier for additional metadata without namespace (everything after last ":")
    identifier_for_additional_metadata = identifier.split(":")[-1]
    additional_metadata = None

    if not is_deleted:
        raw_metadata, additional_metadata = transformation_and_additional_metadata(
            raw_metadata,
            metadata_prefix,
            identifier,
            additional_protocol,
            additional_endpoint,
            additional_format
        )
        if raw_metadata is None:
            return "failed"

    # metadata and record info to be sent to the warehouse
    event_payload = {
        "record_identifier": identifier_for_additional_metadata,
        "datestamp": datestamp,
        "raw_metadata": raw_metadata,
        "additional_metadata": additional_metadata,
        "harvest_url": harvest_url,
        "repo_code": code,
        "harvest_run_id": harvest_run_id,
        "is_deleted": is_deleted
    }

    return "sent" if send_harvest_event(event_payload) else "failed"



def fetch_records_by_id(client, individual_ids, metadata_prefix):
    """
    Yields resolved records for a list of individual identifiers.
    Yields None for any id that fails to resolve or fetch.
    """
    for raw_id in individual_ids:
        resolved_id = resolve_zenodo_identifier(raw_id)
        if resolved_id is None:
            logger.error("Could not resolve identifier %s, skipping.", raw_id)
            yield None
        else:
            try:
                yield client.get_record(identifier=resolved_id, metadata_prefix=metadata_prefix)
            except Exception as e:
                logger.error("Record %s failed: %s", resolved_id, e)
                yield None
        time.sleep(2.1)



def fetch_records_by_sets(client, sets, from_, from_date, until, metadata_prefix):
    """
    Yields records across all configured sets, incremental or full.
    """
    for set_name in sets:
        if from_:
            logger.info("Incremental harvest since %s", from_date)
            records = client.list_records(
                from_ = from_, 
                until = until, 
                metadata_prefix = metadata_prefix, 
                set_ = set_name
            )
        else:
            logger.info("First harvest, fetching all records.")
            records = client.list_records(
                metadata_prefix = metadata_prefix, 
                set_ = set_name, 
                ignore_deleted = True
            )
        yield from records



def run_harvest_loop(record_iter, need_timeout, config, metadata_prefix, harvest_url, code, harvest_run_id, repository_name):
    """
    Consumes an iterator of records, processing and counting each one. Returns (record_count, harvest_events, failed_events).
    """
    record_count = 0
    harvest_events = 0
    failed_events = 0
    for record in record_iter:
        record_count += 1
        if need_timeout and record_count % 10 == 0:
            time.sleep(2)
        if record is None:
            failed_events += 1
            continue
        try:
            status = process_record(record, config, metadata_prefix, harvest_url, code, harvest_run_id, repository_name)
        except Exception as e:
            status = "failed"
            logger.error("Record %s failed: %s", record_count, e)
        if status == "sent":
            harvest_events += 1
        elif status == "failed":
            failed_events += 1
    return record_count, harvest_events, failed_events



def run_harvester_oaipmh(run_info: dict) -> bool:
    """
    Run an OAI-PMH harvest.

    :param run_info (dict): info about the harvest run including: 
        harvest_run_id, 
        from and until dates,
        endpoint_config

    :return bool: True if harvest succeeded, False otherwise
    """

    record_count = 0
    harvest_events = 0
    failed_events = 0

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
        sets = harvest_params.get("set") if harvest_params.get("set") else [None]
        code = config.get("code")
        individual_ids = run_info.get("master_set_identifiers")

        # here we define for which repositories we need to add timeout in order get back all the records from them
        repository_name = config.get("name")
        need_timeout = False
        if repository_name in ["ALBA", "Riga Stradins University", "CLARIN-IV", "ZENODO"]:
            need_timeout = True

        # harvesting
        with Scythe(harvest_url, timeout = 180, max_retries = 3, default_retry_after = 60) as client:

            if individual_ids:
                logger.info("Fetching %d individual records by ID.", len(individual_ids))
                record_iter = fetch_records_by_id(client, individual_ids, metadata_prefix)
            else:
                record_iter = fetch_records_by_sets(client, sets, from_, from_date, until, metadata_prefix)

            record_count, harvest_events, failed_events = run_harvest_loop(
                record_iter, need_timeout, config, metadata_prefix, harvest_url, code, harvest_run_id, repository_name
            )

        logger.info(
            "Harvest summary: processed %s records, successfully sent %s of them to the warehouse, failed to send %s records.",
            record_count,
            harvest_events,
            failed_events
        )

        return failed_events == 0

    except Exception:
        logger.exception("Unexpected error in run_harvester_oaipmh")
        logger.info(
            "Harvest summary: processed %s records, successfully sent %s of them to the warehouse, failed to send %s records.",
            record_count,
            harvest_events,
            failed_events
        )
        return False
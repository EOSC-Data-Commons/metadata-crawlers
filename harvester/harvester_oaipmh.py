import json, logging, os, httpx, time

from urllib.parse import urlparse
from datetime import datetime
from lxml import etree as ET
from oaipmh_scythe import Scythe
from typing import Optional
from collections.abc import Iterable, Iterator
from typing import Any

from .db_api_functions import send_harvest_event

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# shared http client for Dataverse requests
_DATAVERSE_CLIENT = httpx.Client(timeout = 30)



def close_dataverse_client():
    try:
        _DATAVERSE_CLIENT.close()
    except Exception:
        logger.warning("Failed to close Dataverse client")
        pass



def fetch_dataverse_json(doi: str, base_url: str, exporter: str | None) -> Optional[str]:
    """
    Fetch additional metadata: dataverse json

    :param doi: record identifier
    :param base_url: dataverse API endpoint
    :param exporter: exporter type
    :return: stringified JSON with additional metadata; returns None on error
    """
    params = {"exporter": exporter, "persistentId": doi}
    try:
        response = _DATAVERSE_CLIENT.get(base_url, params = params)
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
        response = _DATAVERSE_CLIENT.get(base_url, params = params)
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
            record = client.get_record(identifier = record_id, metadata_prefix = metadata_prefix)
            return ET.tostring(record.xml, pretty_print = True, encoding = "unicode")
    except Exception as e:
        logger.warning("Error fetching %s metadata for %s: %s", metadata_prefix, record_id, e)
        return None
    


def fetch_additional_metadata_zenodo(record_id: str, base_url: str) -> Optional[str]:
    """
    Fetch additional metadata (files) from a Zenodo record via its API.

    :param record_id: Zenodo record identifier is like "oai:zenodo.org:8435696".
    :param base_url: Base URL of Zenodo additional API for file metadata.
    :return: JSON string of the record's file metadata or None if an error occurs.
    """

    # Zenodo OAI IDs come in the form "oai:zenodo.org:8435696".
    # We need only the numeric part at the end for API calls.
    # split(':') -> ['oai', 'zenodo.org', '8435696']
    # [-1] -> '8435696'
    record_id = record_id.split(":")[-1]

    # Construct the full URL to fetch files for this specific record
    # Example: "https://zenodo.org/api/records/8435696/files"
    url = f"{base_url}/{record_id}/files"

    try:
        with httpx.Client() as client:
            response = client.get(url)
            response.raise_for_status()
            return json.dumps(response.json(), indent = 2)

    except httpx.HTTPStatusError as e:
        logger.warning(
            "Failed to fetch Zenodo data for %s: HTTP %s",
            record_id,
            e.response.status_code if e.response else "N/A",
        )
        return None

    except httpx.RequestError as e:
        logger.error(
            "Network error fetching Zenodo data for %s: %s",
            record_id,
            str(e),
        )
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
    


def transformation_and_additional_metadata(raw_metadata: str | None, 
                                           metadata_prefix: str, 
                                           identifier: str, 
                                           additional_protocol: str | None, 
                                           additional_endpoint: str, 
                                           additional_format: str) -> tuple[str | None, str | None]:
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
    :param additional_endpoint (str): Base endpoint URL for additional metadata
    :param additional_format (str): Additional parameter that is needed for some endpoints

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

            if transform is not None:
                if raw_metadata is None:
                    logger.warning("Skipping record %s: no metadata to transform.", identifier)
                    return None, None
                additional_metadata = raw_metadata
                raw_metadata = apply_xslt_transform(raw_metadata, transform)
                if raw_metadata is None:
                    logger.warning("Skipping record %s: transformation to DataCite failed.", identifier)
                    return None, None

        elif additional_protocol == "DATAVERSE_API": # DANS
            additional_metadata = fetch_dataverse_json(
                doi = identifier,
                base_url = additional_endpoint,
                exporter = additional_format
            )

        elif additional_protocol == "OAI-PMH": # DABAR
            additional_metadata = fetch_additional_oai(
                record_id = identifier,
                base_url = additional_endpoint,
                metadata_prefix = additional_format
            )

        elif additional_protocol == "HAL_API": # HAL
            identifier_for_additional_metadata = identifier.split(":")[-1]
            if identifier_for_additional_metadata is None:
                raise ValueError("Incorrect identifier for HAL additional metadata")
            additional_metadata = fetch_additional_metadata_hal(
                record_id = identifier_for_additional_metadata,
                base_url = additional_endpoint
            )

        elif additional_protocol == "ZENODO_API": # ZENODO
            additional_metadata = fetch_additional_metadata_zenodo(
                record_id = identifier,
                base_url = additional_endpoint
            )

    except Exception as e:
        logger.error("Error when fetching additional metadata: %s", e)
        return None, None
    
    return (raw_metadata, additional_metadata)



def resolve_zenodo_identifier(zenodo_doi: str) -> str | None:
    """
    Resolve a Zenodo DOI by following the DOI redirect to the 
    actual Zenodo record page and extracting the record ID.

    :param doi: DOI string, e.g. "10.5281/zenodo.1170128"
    :return: the resolved Zenodo record ID as a string, "skip" if the DOI
             returned HTTP 404, or None if it couldn't be resolved for
             another reason.
    """
    zenodo_doi_url = f"https://doi.org/{zenodo_doi}"

    try:
        response = httpx.get(zenodo_doi_url, follow_redirects = True, timeout = 30)
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            logger.error("DOI %s returned %s, skipping.", zenodo_doi, e.response.status_code)
            return "skip"
        logger.error("Failed to resolve DOI %s: %s", zenodo_doi, e)
        return None
    except httpx.HTTPError as e:
        logger.error("Failed to resolve DOI %s: %s", zenodo_doi, e)
        return None

    resolved_url = str(response.url) # e.g. https://zenodo.org/records/1170128
    path = urlparse(resolved_url).path.rstrip("/") # e.g. "/records/1170128"
    record_id = path.rsplit("/", 1)[-1] # "1170128"

    return f"oai:zenodo.org:{record_id}"



def resolve_zenodo_dois(list_of_zenodo_dois: list[str]) -> set[str]:
    """
    Resolve a list of Zenodo DOIs to their record identifiers.

    :param list_of_zenodo_dois: list of Zenodo DOI strings to resolve
    :return: set of resolved Zenodo identifiers (e.g. "oai:zenodo.org:1170128"),
             deduplicated. DOIs that returned "skip" (404) or that failed
             to resolve for another reason are omitted
    """
    resolved_dois: set[str] = set()

    logger.info("Resolving %d of Zenodo DOIs before fetching records.", len(list_of_zenodo_dois))
    for doi in list_of_zenodo_dois:
        resolved_doi = resolve_zenodo_identifier(doi)

        if resolved_doi is None or resolved_doi == "skip":
            continue

        resolved_dois.add(resolved_doi)
        time.sleep(2.1) # zenodo rate limit is 60 requests per minute so 2.1 * 30 = 63 > 60

    logger.info("Resolved %d/%d DOIs.", len(resolved_dois), len(list_of_zenodo_dois))
    return resolved_dois



def process_record(record, config: dict, metadata_prefix: str, harvest_url: str, code: str, harvest_run_id: str, repository_name: str) -> str:
    """
    Process a single OAI-PMH record: extract and transform its metadata,
    then send the resulting event to the warehouse.

    :param record: OAI-PMH record object (as returned by Scythe)
    :param config: harvest endpoint configuration dict
    :param metadata_prefix: OAI-PMH metadata prefix the record was harvested with
                             (e.g. "oai_dc", "datacite")
    :param harvest_url: base URL of the OAI-PMH endpoint the record came from
    :param code: repository code
    :param harvest_run_id: identifier of the current harvest run
    :param repository_name: repository name
    :return str: "skipped" if the record was excluded (e.g. ALBA empty sets)
                 "failed" if metadata transformation failed or the event could not be sent to the warehouse
                 "sent" if the event was successfully sent
    """
    try:
        identifier = record.header.identifier
        datestamp = record.header.datestamp
        is_deleted = getattr(record.header, "status", None) == "deleted"
        raw_metadata = ET.tostring(record.xml, pretty_print=True, encoding = "unicode")

        # special case where we skip some records for PaNOSC ALBA repository because those records have poor metadata
        if repository_name == "ALBA":
            setSpecs = record.header.setSpecs
            if setSpecs == []:
                return "skipped"

        additional_protocol = None
        additional_endpoint = ""
        additional_format = ""
        harvest_params = config.get("harvest_params")
        if harvest_params:
            additional = harvest_params.get("additional_metadata_params")
            if additional:
                additional_protocol = additional.get("protocol")
                additional_endpoint = additional["endpoint"]
                additional_format = additional["format"]

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
    except Exception as e:
        logger.error("Processing of record failed: %s", e)
        return "failed"



def fetch_records_by_id(client, record_ids: set[str], metadata_prefix: str, repository_name: str) -> Iterator[Any]:
    """
    Fetch records for a list of IDs

    :param client: an active Scythe OAI-PMH client used to fetch individual records
    :param ids: iterable of record identifiers to fetch
    :param metadata_prefix: OAI-PMH metadata prefix to request (e.g. "oai_dc", "datacite")
    :param repository_name: repository_name
    :return: list of successfully fetched record objects
    """
    logger.info("Fetching records for %d identifiers", len(record_ids))
    for id in record_ids:
        try:
            yield client.get_record(identifier = id, metadata_prefix = metadata_prefix)
            if repository_name == "Zenodo":
                time.sleep(2.1)  # zenodo rate limit is 60 requests per minute so 2.1 * 30 = 63 > 60
        except Exception as e:
            logger.error("Record %s failed: %s", id, e)



def fetch_records_by_sets(client, sets: Iterable[str | None], from_: str | None, 
                          from_date: str | None, until_: str | None, metadata_prefix: str) -> Iterator[Any]:
    """
    Yield OAI-PMH records across all configured sets, either incrementally (from_ to until_) or as a full harvest.

    :param client: an active Scythe OAI-PMH client used to list records
    :param sets: iterable of OAI-PMH set names to harvest
    :param from_: lower bound datestamp
    :param from_date: original, unformatted "from" date used only for logging
    :param until_: upper bound datestamp
    :param metadata_prefix: OAI-PMH metadata prefix to request (e.g. "oai_dc", "datacite")
    :return: generator yielding record objects across all given sets
    """
    sets = sets or [None]

    for set_name in sets:
        if from_:
            logger.info("Incremental harvest since %s", from_date)
            records = client.list_records(
                from_ = from_, 
                until = until_, 
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



def run_harvest_loop(record_iter, need_timeout: bool, config: dict, metadata_prefix: str, 
                     harvest_url: str, code: str, harvest_run_id: str, repository_name: str
                     ) -> tuple[int, int, int]:
    """
    Consume an iterator of records, processing and counting each one.

    :param record_iter: iterable of record objects to process
    :param need_timeout: if True, sleep 2 seconds after every 10th record
                          to throttle requests to rate-limited repositories
    :param config: harvest endpoint configuration dict
    :param metadata_prefix: OAI-PMH metadata prefix used for this harvest
    :param harvest_url: base URL of the OAI-PMH endpoint
    :param code: repository code
    :param harvest_run_id: identifier of the current harvest run
    :param repository_name: repository name
    :return: tuple of (record_count, harvest_events, failed_events) where
             record_count is the total number of items consumed from
             record_iter, harvest_events is the number successfully sent
             to the warehouse, and failed_events is the number that were
             None, raised an exception, or returned "failed"
    """
    record_count = 0
    harvest_events = 0
    failed_events = 0
    for record in record_iter:
        record_count += 1
        if record is None:
            failed_events += 1
            continue
        if need_timeout and record_count % 10 == 0:
            time.sleep(2.1)

        status = process_record(record, config, metadata_prefix, harvest_url, code, harvest_run_id, repository_name)

        if status == "sent":
            harvest_events += 1
        elif status == "failed":
            failed_events += 1
        else:
            continue

    return record_count, harvest_events, failed_events



def run_harvester_oaipmh(run_info: dict) -> bool:
    """
    Run an OAI-PMH harvest.

    :param run_info (dict): info about the harvest run
    :return bool: True if harvest succeeded, False otherwise
    """
    record_count = 0
    harvest_events = 0
    failed_events = 0

    try:
        # extract run info and harvest params
        harvest_run_id = run_info.get("id")
        if harvest_run_id is None:
            raise ValueError("Missing harvest run ID")

        # extract dates
        from_date = run_info.get("from_date")
        from_ = datetime.strptime(from_date, '%Y-%m-%dT%H:%M:%S.%f%z').strftime('%Y-%m-%dT%H:%M:%SZ') if from_date else None
        until_date = run_info.get("until_date")
        if until_date is None:
            raise ValueError("Missing until date")
        until_ = datetime.strptime(until_date, '%Y-%m-%dT%H:%M:%S.%f%z').strftime('%Y-%m-%dT%H:%M:%SZ')

        config = run_info.get("endpoint_config")
        if config is None:
            raise ValueError("Missing config")
        
        # extract parameters from config
        harvest_url = config.get("harvest_url")
        if harvest_url is None:
            raise ValueError("Missing harvest url")

        harvest_params = config.get("harvest_params")
        if harvest_params is None:
            raise ValueError("Missing harvest parameters")

        code = config.get("code")
        if code is None:
            raise ValueError("Missing code of repository")

        # extract additional parameters from harvest_params
        metadata_prefix = harvest_params.get("metadata_prefix", "oai_dc")
        sets = harvest_params.get("set")

        # if master_set_indentifiers is defined then we do harvest by individual IDs
        individual_ids = run_info.get("master_set_identifiers")

        # here we define for which repositories we need to add timeout in order get back all the records from them
        repository_name = config.get("name")
        if repository_name is None:
            raise ValueError("Missing name of repository")
    
        need_timeout = False
        if repository_name in ["ALBA", "Riga Stradins University", "CLARIN-IV", "Zenodo"]:
            need_timeout = True

        # harvesting
        with Scythe(harvest_url, timeout = 180, max_retries = 3, default_retry_after = 60) as client:

            if individual_ids: # currently only used for HAL-Zenodo
                resolved_ids = resolve_zenodo_dois(individual_ids)
                record_iter = fetch_records_by_id(client, resolved_ids, metadata_prefix, repository_name)
            else:
                record_iter = fetch_records_by_sets(client, sets, from_, from_date, until_, metadata_prefix)

            record_count, harvest_events, failed_events = run_harvest_loop(
                record_iter, need_timeout, config, metadata_prefix, harvest_url, code, harvest_run_id, repository_name
            )

        logger.info(
            "Harvest summary: processed %s records, successfully sent %s of them to the warehouse, failed to send %s records",
            record_count,
            harvest_events,
            failed_events
        )

        return failed_events == 0

    except Exception:
        logger.exception("Unexpected error in run_harvester_oaipmh")
        logger.info(
            "Harvest summary: processed %s records, successfully sent %s of them to the warehouse, failed to send %s records",
            record_count,
            harvest_events,
            failed_events
        )
        return False
import httpx
from httpx_retries import Retry, RetryTransport
import asyncio
import logging
import json
from typing import List, Dict, Generator, AsyncGenerator, Tuple
import os
from datetime import datetime, timezone, date
from lxml import etree
from harvester.settings import settings
from harvester.db_api_functions import send_harvest_event

ACCESS_TOKEN = settings.FINBIF_ACCESS_TOKEN

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Base URL and config for the FinBIF API
API_BASE = "https://api.laji.fi"
COLLECTIONS_PAGE_SIZE = 1000
SUBCOLLECTIONS_PAGE_SIZE = 1000
MAX_CONCURRENT_REQUESTS = 5

retry_strategy = Retry(
    total=8,  # Number of retries
    backoff_factor=0.5,  # Delay between retries (exponential backoff)
)
_FINBIF_CLIENT = httpx.Client(
    transport=RetryTransport(retry=retry_strategy),
    timeout=httpx.Timeout(120),
    headers={"Authorization": f"Bearer {ACCESS_TOKEN}"}
)
_ASYNC_FINBIF_CLIENT = httpx.AsyncClient(
    headers={"Authorization": f"Bearer {ACCESS_TOKEN}"},
    transport=RetryTransport(retry=retry_strategy),
    timeout=httpx.Timeout(120),
)

def shutdown_client():
    try:
        _FINBIF_CLIENT.close()
        logger.info("Client closed successfully.")
    except Exception as e:
        logger.error("Error closing client: %s", e)

async def shutdown_async_client():
    """
    Shutdown the shared async client.
    """
    try:
        await _ASYNC_FINBIF_CLIENT.aclose()
        logger.info("Async client closed successfully.")
    except Exception as e:
        logger.error("Error closing async client: %s", e)


# GET COLLECTIONS:
def fetch_collections() -> Generator[Dict, None, None]:
    """
    Fetch all collections from the FinBIF API.

    :yield: A collection record as a dictionary.
    """
    page = 1
    while True:
        params = {"lang": "en", "page": page, "pageSize": COLLECTIONS_PAGE_SIZE}
        response = _FINBIF_CLIENT.get(f"{API_BASE}/collections", params=params)
        response.raise_for_status()
        data = response.json()

        # Yield results from the current page
        for result in data.get("results", []):
            yield result

        # Check if there are more pages
        if "nextPage" not in data or not data["results"]:
            break
        page += 1

def process_collections(collections: List[Dict]) -> List[Dict]:
    """
    Process the collections to filter out those without children, and extract relevant metadata.

    :param collections: A list of collections as dictionaries.
    :return: A list of filtered collections with relevant metadata.
    """
    filtered_collections = []
    for collection in collections:
        if not collection.get("hasChildren", False):
            # Extract relevant metadata
            filtered_collections.append({
                "collection_id": collection.get("id"),
                "collection_name": collection.get("collectionName"),
                "collection_long_name": collection.get("longName"),
                "collection_size": collection.get("collectionSize"),
                "collection_type": collection.get("collectionType", "").removeprefix("MY.collectionType"),
                "description": collection.get("description"),
                "data_quality_description": collection.get("dataQualityDescription"),
                "language": collection.get("language"),
                "publisher_shortname": collection.get("publisherShortname"),
                "intellectual_owner": collection.get("intellectualOwner"),
                "taxonomic_coverage": collection.get("taxonomicCoverage"),
                "geographic_coverage": collection.get("geographicCoverage"),
                "temporal_coverage": collection.get("temporalCoverage"),
                "date_created": collection.get("dateCreated"),
                "date_edited": collection.get("dateEdited"),
            })
    return filtered_collections


# GET SUBCOLLECTIONS:
async def fetch_subcollection_page(collection_id: str, page: int) -> dict:
    """
    Fetch a single page of subcollections for a given collection.

    :param client: The AsyncClient instance.
    :param collection_id: The ID of the collection.
    :param page: The page number to fetch.
    :return: The JSON response as a dictionary.
    """
    url = f"{API_BASE}/warehouse/query/unit/aggregate"
    aggregate_by = [
    "gathering.conversions.year",                       # year observations were collected
    "gathering.country",                                # country of observation (or country code)
    "gathering.interpretations.country",                # FinBIF country code
    "gathering.interpretations.countryDisplayname",     # country name in Finnish
    "gathering.interpretations.finnishMunicipality",    # FinBIF municipality code (only for Finnish municipalities)
    "gathering.interpretations.municipalityDisplayname",# Finnish municipality name
    "gathering.municipality",                           # municipality name as provided by the observer (free text, non-standardized)
    "unit.linkings.taxon.id",                           # FinBIF taxon ID
    "unit.linkings.taxon.nameEnglish",                  # taxon name in English
    "unit.linkings.taxon.nameFinnish",                  # taxon name in Finnish
    "unit.linkings.taxon.nameSwedish",                  # taxon name in Swedish
    "unit.linkings.taxon.scientificName",               # taxon scientific name
]
    params = {
        "aggregateBy": ",".join(aggregate_by),
        "onlyCount": False,
        "pageSize": SUBCOLLECTIONS_PAGE_SIZE,
        "collectionId": collection_id,
        "page": page,
        "orderBy": "firstLoadDateMax DESC"
    }
    response = await _ASYNC_FINBIF_CLIENT.get(url, params=params)
    response.raise_for_status()
    return response.json()

async def iter_subcollections(collection_id: str, from_date: date | None) -> AsyncGenerator[Dict, None]:
    """
    Stream subcollections page-by-page using the existing fetch_subcollection_page() function and semaphore logic.
    Use concurrent page fetching for initial harvest and sequential fetching for incremental harvest, stopping when firstLoadDateMax is older than from_date.
    """
    # Fetch the first page to determine the total number of pages
    first_page = await fetch_subcollection_page(collection_id, page=1)
    total_pages = first_page.get("lastPage", 1)
    logger.info("Collection ID %s has %d pages of subcollections.", collection_id, total_pages)

    # incremental harvest mode
    if from_date is not None:
        page = 1
        while page <= total_pages:
            if page == 1:
                page_data = first_page
            else:
                page_data = await fetch_subcollection_page(collection_id, page)

            results = page_data.get("results", [])

            if not results:
                return

            for result in results:
                # date when the latest observation was added
                first_load_str = result.get("firstLoadDateMax")
                if first_load_str:
                    record_date = datetime.strptime(first_load_str, "%Y-%m-%d").date()

                    # early stop condition
                    if record_date < from_date:
                        logger.info("Stopping subcollection fetch for collection %s at page %d and date %s because subsequent records are older than from_date %s.", collection_id, page, record_date, from_date)
                        return

                yield result

            page += 1

    # initial harvest mode:
    else:
        # Yield first page results
        for result in first_page.get("results", []):
            yield result

        if total_pages == 1:
            return

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        async def fetch_with_semaphore(page: int):
            async with semaphore:
                return await fetch_subcollection_page(collection_id, page)

        # Schedule remaining pages
        tasks = [
            fetch_with_semaphore(page)
            for page in range(2, total_pages + 1)
        ]

        for task  in asyncio.as_completed(tasks):
            page_data = await task 
            for result in page_data.get("results", []):
                yield result


# CREATE RECORDS FROM SUBCOLLECTIONS:
async def create_records(collection: Dict, from_date: date | None) -> AsyncGenerator[Dict, None]:
    """
    Create records from collection metadata and its subcollections.

    :param collection: A collection dictionary.
    :return: Async generator yielding records.
    """
    parent_id = collection["collection_id"]
    logger.info("Fetching subcollections for collection ID: %s", parent_id)

    async for subcollection in iter_subcollections(parent_id, from_date):
        record = {
            "gathering_year": subcollection['aggregateBy'].get("gathering.conversions.year"),
            "gathering_country": subcollection['aggregateBy'].get("gathering.country"),
            "gathering_country_code": subcollection['aggregateBy'].get("gathering.interpretations.country", "").removeprefix("http://tun.fi/"),
            "gathering_country_finnish": subcollection['aggregateBy'].get("gathering.interpretations.countryDisplayname"),
            "gathering_municipality": subcollection['aggregateBy'].get("gathering.municipality"),
            "gathering_municipality_code": subcollection['aggregateBy'].get("gathering.interpretations.finnishMunicipality", "").removeprefix("http://tun.fi/"),
            "gathering_municipality_finnish": subcollection['aggregateBy'].get("gathering.interpretations.municipalityDisplayname"),
            "species_code": subcollection['aggregateBy'].get("unit.linkings.taxon.id", "").removeprefix("http://tun.fi/").replace("https://www.gbif.org/species/", "gbif:"),
            "species_scientific_name": subcollection['aggregateBy'].get("unit.linkings.taxon.scientificName"),
            "species_english_name": subcollection['aggregateBy'].get("unit.linkings.taxon.nameEnglish"),
            "species_finnish_name": subcollection['aggregateBy'].get("unit.linkings.taxon.nameFinnish"),
            "species_swedish_name": subcollection['aggregateBy'].get("unit.linkings.taxon.nameSwedish"),
            "count": subcollection.get("count"),
            "oldest_record": subcollection.get("oldestRecord"),
            "newest_record": subcollection.get("newestRecord"),
            "first_date_added": subcollection.get("firstLoadDateMin"),
            "last_date_added": subcollection.get("firstLoadDateMax")
        }
        record.update(collection)
        yield record


#PROCESS RECORDS:
def finbif_dict_to_xml(record: dict) -> str:
    """
    Convert a dictionary record to an XML (to be sent to XSLT).

    :param record: A dictionary containing the record data.
    :return: A string containing the XML representation of the record.
    """
    root = etree.Element("record")

    for key, value in record.items():
        if value is None or value == "":
            continue
        el = etree.SubElement(root, key)
        el.text = str(value)

    return etree.tostring(root, pretty_print=True, encoding="UTF-8").decode()

def apply_xslt_transform(xml_record: str, transform) -> Tuple[str, str]:
    """
    Apply a precompiled XSLT transform to an XML record.

    :param xml_record: FinBIF XML record as string
    :param transform: Compiled lxml.etree.XSLT object
    :return: 
        - Datacite XML string
        - Extracted identifier string
    """
    try:
        record = etree.fromstring(xml_record.encode("utf-8"))
        result_tree = transform(record)
        identifier_nodes = result_tree.xpath(
            "//*[local-name()='identifier']"
        )
        identifier = identifier_nodes[0].text if identifier_nodes else None
        datacite_xml = etree.tostring(result_tree, pretty_print=True, encoding="UTF-8").decode("utf-8")
        return datacite_xml, identifier
    except Exception as e:
        logger.warning("Transformation failed: %s", e)
        return None, None
    

# MAIN HARVESTER:
async def harvest_finbif(run_info: dict) -> bool:
    """
    Function to fetch and process collections from the FinBIF API.
    """

    try:
        record_counter = 0
        harvest_events = 0
        failed_events = 0

        harvest_run_id = run_info.get("id")
        from_date = run_info.get("from_date")
        from_ = datetime.fromisoformat(from_date.replace("Z", "+00:00")).date() if from_date else None

        if from_:
            logger.info("Incremental harvest since %s", from_date)
        else:
            logger.info("First harvest, fetching all records.")

        # Fetch all collections
        all_collections = list(fetch_collections())
        logger.info("Fetched %d collections.", len(all_collections))
        # Process collections
        filtered_collections = process_collections(all_collections)
        # Log the number of collections without children
        logger.info("Found %s collections without children.", len(filtered_collections))

        XSLT_PATH = os.path.join(BASE_DIR, "finbif_to_datacite.xsl")
        xslt_doc = etree.parse(XSLT_PATH)
        transform = etree.XSLT(xslt_doc)

        success = True

        for collection in filtered_collections:
            async for record in create_records(collection, from_):
                record_counter += 1
                if record_counter % 1000 == 0:
                    logger.info("Processed %d records so far", record_counter)

                xml_record = finbif_dict_to_xml(record)
                datacite_record, record_identifier = apply_xslt_transform(
                    xml_record,
                    transform
                )

                if not record_identifier:
                    logger.warning("Missing identifier, skipping record")
                    continue

                if not datacite_record:
                    logger.warning(
                        "Failed DataCite transform for %s:%s:%s:%s",
                        record['collection_id'],
                        record['gathering_year'],
                        record.get('gathering_municipality_code') or record.get('gathering_country_code'),
                        record['species_code']
                    )
                    continue

                try:
                    event_payload = {
                        "record_identifier": record_identifier,
                        "datestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                        "raw_metadata": datacite_record,
                        "metadata_format": "XML",
                        "additional_metadata": json.dumps(record),
                        "harvest_url": API_BASE,
                        "repo_code": "FINBIF",
                        "harvest_run_id": harvest_run_id,
                        "is_deleted": False,
                    }

                    if send_harvest_event(event_payload):
                        harvest_events += 1
                    else:
                        failed_events += 1

                except Exception as e:
                    logger.error("Unexpected error while processing record %s: %s", record_identifier, e)
                    success = False

        logger.info(
            "Harvest summary: processed %s records, successfully sent %s of them to the warehouse, failed to send %s records.",
            record_counter,
            harvest_events,
            failed_events
        )

        return success


    except httpx.RequestError as e:
        logger.error("Network error while fetching collections: %s", e)
        return False
    except httpx.HTTPStatusError as e:
        logger.error("HTTP error while fetching collections: %s", e)
        return False
    except Exception as e:
        logger.error("Unexpected error: %s", e)
        return False

    finally:
        shutdown_client()
        await shutdown_async_client()


def run_harvester_finbif(run_info: dict) -> bool:
    """
    Entry point for FinBIF harvesting from main.py
    """
    try:
        return asyncio.run(harvest_finbif(run_info))
    except Exception as e:
        logger.exception("FinBIF harvester crashed: %s", e)
        return False

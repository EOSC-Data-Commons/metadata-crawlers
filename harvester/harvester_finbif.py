import httpx
from httpx_retries import Retry, RetryTransport
import asyncio
import logging
import json
from typing import List, Dict, Generator
import os
from datetime import datetime, timezone
from lxml import etree
from harvester.settings import settings
from harvester.db_api_functions import send_harvest_event

ACCESS_TOKEN = settings.FINBIF_ACCESS_TOKEN

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Base URL for the FinBIF API
API_BASE = "https://api.laji.fi"
COLLECTIONS_PAGE_SIZE = 1000
SUBCOLLECTIONS_PAGE_SIZE = 1000
MAX_CONCURRENT_REQUESTS = 1

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

async def shutdown_async_client():
    """
    Shutdown the shared async client.
    """
    try:
        await _ASYNC_FINBIF_CLIENT.aclose()
        logger.info("Async client closed successfully.")
    except Exception as e:
        logger.error("Error closing async client: %s", e)

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
        "page": page
    }
    response = await _ASYNC_FINBIF_CLIENT.get(url, params=params)
    response.raise_for_status()
    return response.json()


async def fetch_all_subcollections(collection_id: str) -> List[Dict]:
    """
    Fetch all subcollections for a given collection, using parallel requests.

    :param collection_id: The ID of the collection.
    :return: A list of all subcollections.
    """
    # Fetch the first page to determine the total number of pages
    first_page = await fetch_subcollection_page(collection_id, page=1)
    total_pages = first_page.get("lastPage", 1)
    logger.info("Collection ID %s has %d pages of subcollections.", collection_id, total_pages)
    subcollections = first_page.get("results", [])

    # Semaphore to limit the number of concurrent requests
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async def fetch_with_semaphore(page):
        async with semaphore:
            page_data = await fetch_subcollection_page(collection_id, page)
            return page_data.get("results", [])

    # Create tasks for the remaining pages
    tasks = [fetch_with_semaphore(page) for page in range(2, total_pages + 1)]
    results = await asyncio.gather(*tasks)

    # Combine results from all pages
    for page_results in results:
        subcollections.extend(page_results)

    logger.info("Fetched %d subcollections for collection ID: %s", len(subcollections), collection_id)
    return subcollections


async def create_records(collection: Dict) -> Generator[Dict, None, None]:
    """
    Create records from collection metadata and its subcollections.

    :param collection: A collection dictionary.
    :return: A list of records combining collection and subcollection data.
    """
    parent_id = collection["collection_id"]
    logger.info("Fetching subcollections for collection ID: %s", parent_id)
    subcollections = await fetch_all_subcollections(parent_id)

    for subcollection in subcollections:
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
            "newest_record": subcollection.get("newestRecord")
        }
        record.update(collection)
        yield record


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

def apply_xslt_transform(xml_record: str, transform: etree.XSLT) -> str | None:
    """
    Apply a precompiled XSLT transform to an XML record.

    :param xml_record: FinBIF XML record as string
    :param transform: Compiled lxml.etree.XSLT object
    :return: Transformed XML as string, or None on failure
    """
    try:
        record = etree.fromstring(xml_record.encode("utf-8"))
        result_tree = transform(record)
        return etree.tostring(result_tree, pretty_print=True, encoding="UTF-8").decode("utf-8")
    except Exception as e:
        logger.warning("Transformation failed: %s", e)
        return None

def extract_identifier(datacite_xml: str) -> str | None:
    try:
        root = etree.fromstring(datacite_xml.encode())
        return root.xpath("string(//identifier)")
    except Exception:
        return None
    

async def harvest_finbif(run_info: dict) -> bool:
    """
    Function to fetch and process collections from the FinBIF API.
    """

    try:
        harvest_run_id = run_info.get("id")
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
        record_counter = 0

        for collection in filtered_collections:
            async for record in create_records(collection):
                record_counter += 1
                if record_counter % 1000 == 0:
                    logger.info("Processed %d records so far", record_counter)

                xml_record = finbif_dict_to_xml(record)
                datacite_record = apply_xslt_transform(xml_record, transform)

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
                        "record_identifier": extract_identifier(datacite_record),
                        "datestamp": datetime.now(timezone.utc).isoformat(),
                        "raw_metadata": datacite_record,
                        "additional_metadata": record,
                        "harvest_url": API_BASE,
                        "repo_code": "FINBIF",
                        "harvest_run_id": harvest_run_id,
                        "is_deleted": False,
                    }

                    send_harvest_event(event_payload)

                except Exception as e:
                    logger.error("Failed to send record: %s", e)
                    success = False

        return success


    except httpx.RequestError as e:
        logger.error("Network error while fetching collections: %s", e)
    except httpx.HTTPStatusError as e:
        logger.error("HTTP error while fetching collections: %s", e)
    except Exception as e:
        logger.error("Unexpected error: %s", e)

    finally:
        await shutdown_async_client()
        _FINBIF_CLIENT.close()


def run_harvester_finbif(run_info: dict) -> bool:
    """
    Entry point for FinBIF harvesting from main.py
    """
    try:
        return asyncio.run(harvest_finbif(run_info))
    except Exception as e:
        logger.exception("FinBIF harvester crashed: %s", e)
        return False

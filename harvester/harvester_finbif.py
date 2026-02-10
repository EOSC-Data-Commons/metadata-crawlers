import httpx
from httpx import Limits, Timeout, Client
from httpx_retries import Retry, RetryTransport
import logging
import json
from typing import List, Dict, Generator

logger = logging.getLogger(__name__)

# Base URL for the FinBIF API
API_BASE = "https://api.laji.fi"

retry_strategy = Retry(
    total=8,  # Number of retries
    backoff_factor=0.5,  # Delay between retries (exponential backoff)
)
retry_transport = RetryTransport(retry=retry_strategy)
_FINBIF_CLIENT = Client(
    transport=retry_transport,
    timeout=Timeout(120)
)

def fetch_results(api_url: str, params: Dict) -> Generator[Dict, None, None]:
    """
    Fetch all collections or subcollections from the FinBIF API, iterating through pages.

    :param api_url: The base URL for the API endpoint.
    :param params: Query parameters for the API request.
    :yield: A (sub)collection record as a dictionary.
    """
    page = 1
    while True:
        params["page"] = page
        response = _FINBIF_CLIENT.get(api_url, params=params)
        response.raise_for_status()
        data = response.json()

        # Yield results from the current page
        for result in data.get("results", []):
            yield result

        # Check if there are more pages
        if not data.get("nextPage"):
            break
        page += 1

def fetch_all_collections() -> List[Dict]:
    """
    Fetch all collections from the FinBIF API.

    :return: A list of all collections as dictionaries.
    """
    api_url = f"{API_BASE}/collections"
    params = {
        "lang": "en",
        "pageSize": 100
    }
    return list(fetch_results(api_url, params))

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
                "id": collection.get("id"),
                "collectionName": collection.get("collectionName"),
                "longName": collection.get("longName"),
                "collectionSize": collection.get("collectionSize"),
                "collectionType": collection.get("collectionType"),
                "description": collection.get("description"),
                "language": collection.get("language"),
                "owner": collection.get("owner"),
                "publisherShortname": collection.get("publisherShortname"),
                "intellectualOwner": collection.get("intellectualOwner"),
                "taxonomicCoverage": collection.get("taxonomicCoverage"),
                "geographicCoverage": collection.get("geographicCoverage"),
                "temporalCoverage": collection.get("temporalCoverage"),
                "dateCreated": collection.get("dateCreated"),
                "dateEdited": collection.get("dateEdited"),
            })
    return filtered_collections

def fetch_subcollections(parent_id: str) -> List[Dict]:
    """
    Fetch subcollections for a given parent collection ID.

    :param parent_id: The ID of the parent collection.
    :return: A list of subcollections as dictionaries.
    """
    api_url = f"{API_BASE}/warehouse/query/unit/aggregate"
    params = {
        "aggregateBy": "gathering.conversions.year,gathering.municipality,unit.linkings.originalTaxon.scientificName",
        "onlyCount": False,
        "pageSize": 100,
        "collectionId": parent_id
    }
    results = list(fetch_results(api_url, params))
    print(f"Fetched {len(results)} subcollections for parent ID: {parent_id}")
    return results

def create_records(collection: Dict) -> List[Dict]:
    """
    Create records from collection metadata and its subcollections.

    :param collection: A collection dictionary.
    :return: A list of records combining collection and subcollection data.
    """
    parent_id = collection["id"]
    print(f"Fetching subcollections for collection ID: {parent_id}")
    subcollections = fetch_subcollections(parent_id)
    records = []

    for subcollection in subcollections:
        record = {
            "gathering_year": subcollection['aggregateBy'].get("gathering.conversions.year"),
            "gathering_municipality": subcollection['aggregateBy'].get("gathering.municipality"),
            "scientific_name": subcollection['aggregateBy'].get("unit.linkings.originalTaxon.scientificName"),
            "count": subcollection.get("count"),
            "oldest_record": subcollection.get("oldestRecord"),
            "newest_record": subcollection.get("newestRecord")
        }
        record.update(collection)
        records.append(record)
        print(f"Broj records u subcoll: {len(records)}")
        if len(records) >= 10:
            print(f"Reached 10 records for collection {parent_id}, stopping further processing.")
            logger.info("Reached 10 records for collection %s, stopping further processing.", parent_id)
            break

    return records

def main():
    """
    Main function to fetch and process collections from the FinBIF API.
    """
    logging.basicConfig(
        level=logging.INFO, 
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    try:
        # Fetch all collections
        all_collections = fetch_all_collections()
        # Process collections
        filtered_collections = process_collections(all_collections)
        # Log the number of collections without children
        print(f"Found {len(filtered_collections)} collections without children")
        logger.info("Found %s collections without children.", len(filtered_collections))

        # Fetch subcollections for each filtered collection and create metadata records
        records = []
        for collection in filtered_collections:
            records.extend(create_records(collection))
            print(f"Ukupni broj records: {len(records)}")
            if len(records) >= 100:
                logger.info("Reached 100 total records, stopping further processing.")
                break

        with open('outputfile', 'w') as file:
            file.write(json.dumps(records, indent=4))

    except httpx.RequestError as e:
        logger.error("Network error while fetching collections: %s", e)
    except httpx.HTTPStatusError as e:
        logger.error("HTTP error while fetching collections: %s", e)
    except Exception as e:
        logger.error("Unexpected error: %s", e)

if __name__ == "__main__":
    main()
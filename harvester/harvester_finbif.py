import httpx
from httpx_retries import Retry, RetryTransport
import asyncio
import logging
import json
from typing import List, Dict, Generator
import os
from harvester.settings import settings

ACCESS_TOKEN = os.getenv("FINBIF_ACCESS_TOKEN")
#ACCESS_TOKEN = settings.FINBIF_ACCESS_TOKEN

logger = logging.getLogger(__name__)

# Base URL for the FinBIF API
API_BASE = "https://api.laji.fi"
COLLECTIONS_PAGE_SIZE = 100
SUBCOLLECTIONS_PAGE_SIZE = 100
MAX_CONCURRENT_REQUESTS = 10

retry_strategy = Retry(
    total=8,  # Number of retries
    backoff_factor=0.5,  # Delay between retries (exponential backoff)
)
_FINBIF_CLIENT = httpx.Client(
    transport=RetryTransport(retry=retry_strategy),
    timeout=httpx.Timeout(120),
    headers={"Authorization": f"Bearer {ACCESS_TOKEN}"}
)

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

async def fetch_subcollection_page(client: httpx.AsyncClient, collection_id: str, page: int) -> dict:
    """
    Fetch a single page of subcollections for a given collection.

    :param client: The AsyncClient instance.
    :param collection_id: The ID of the collection.
    :param page: The page number to fetch.
    :return: The JSON response as a dictionary.
    """
    url = f"{API_BASE}/warehouse/query/unit/aggregate"
    params = {
        "aggregateBy": "gathering.conversions.year,gathering.municipality,unit.linkings.originalTaxon.scientificName",
        "onlyCount": False,
        "pageSize": SUBCOLLECTIONS_PAGE_SIZE,
        "collectionId": collection_id,
        "page": page
    }
    response = await client.get(url, params=params)
    response.raise_for_status()
    return response.json()


async def fetch_all_subcollections(collection_id: str) -> List[Dict]:
    """
    Fetch all subcollections for a given collection, using parallel requests.

    :param collection_id: The ID of the collection.
    :return: A list of all subcollections.
    """
    async with httpx.AsyncClient(
        headers={"Authorization": f"Bearer {ACCESS_TOKEN}"},
        transport=RetryTransport(retry_strategy=retry_strategy),
        timeout=httpx.Timeout(120)
    ) as client:
        # Fetch the first page to determine the total number of pages
        first_page = await fetch_subcollection_page(client, collection_id, page=1)
        total_pages = first_page.get("lastPage", 1)
        subcollections = first_page.get("results", [])

        # Semaphore to limit the number of concurrent requests
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        async def fetch_with_semaphore(page):
            async with semaphore:
                page_data = await fetch_subcollection_page(client, collection_id, page)
                return page_data.get("results", [])

        # Create tasks for the remaining pages
        tasks = [fetch_with_semaphore(page) for page in range(2, total_pages + 1)]
        results = await asyncio.gather(*tasks)

        # Combine results from all pages
        for page_results in results:
            subcollections.extend(page_results)

        return subcollections


def create_records(collection: Dict) -> List[Dict]:
    """
    Create records from collection metadata and its subcollections.

    :param collection: A collection dictionary.
    :return: A list of records combining collection and subcollection data.
    """
    parent_id = collection["id"]
    print(f"Fetching subcollections for collection ID: {parent_id}")
    subcollections = fetch_all_subcollections(parent_id)
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
        all_collections = List(fetch_collections())
        logger.info("Fetched %d collections.", len(all_collections))
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
import httpx
import logging
from typing import List, Dict, Generator

logger = logging.getLogger(__name__)

# Base URL for the FinBIF API
API_BASE = "https://laji.fi/api"

_FINBIF_CLIENT = httpx.Client(timeout=30)

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

def process_collections(collections: List[Dict]) -> List[Dict]:
    """
    Process the collections to filter out those without children.

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



def main():
    """
    Main function to fetch and process collections from the FinBIF API.
    """
    try:
        # Fetch all collections
        api_url = f"{API_BASE}/collections"
        params = {
            "lang": "en",
            "pageSize": 100
        }
        all_collections = list(fetch_results(api_url, params))
        # Process collections
        filtered_collections = process_collections(all_collections)
        # Log the number of collections without children
        logger.info("Found %s collections without children.", len(filtered_collections))

        # For now, just print the first three filtered collections
        print(filtered_collections[slice(3)])

        # Fetch subcollections for each filtered collection
        api_url = f"{API_BASE}/warehouse/query/unit/aggregate"
        params = {
            "aggregateBy": "gathering.conversions.year,gathering.municipality,unit.linkings.originalTaxon.scientificName",
            "onlyCount": False,
            "pageSize": 100
        }
        records = []
        for collection in filtered_collections:
            parent_id = collection["id"]
            params["collectionId"] = parent_id
            subcollections = list(fetch_results(api_url, params))
            logger.info("Collection ID %s has %s subcollections.", parent_id, len(subcollections))

            for subcollection in subcollections:
                record = {
                    "gathering_year": subcollection.get("gathering.conversions.year"),
                    "gathering_municipality": subcollection.get("gathering.municipality"),
                    "scientific_name": subcollection.get("unit.linkings.originalTaxon.scientificName"),
                    "count": subcollection.get("count"),
                    "oldest_record": subcollection.get("oldestRecord"),
                    "newest_record": subcollection.get("newestRecord")
                }
                record.update(collection)
                records.append(record)



    except httpx.RequestError as e:
        logger.error("Network error while fetching collections: %s", e)
    except httpx.HTTPStatusError as e:
        logger.error("HTTP error while fetching collections: %s", e)
    except Exception as e:
        logger.error("Unexpected error: %s", e)

if __name__ == "__main__":
    main()
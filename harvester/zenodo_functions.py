from urllib.parse import urlparse
import logging
import httpx
import time

logger = logging.getLogger(__name__)

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
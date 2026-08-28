import logging, os, time, json, re, xml.etree.ElementTree as ET, requests
from .db_api_functions import send_harvest_event
from xml.dom import minidom
from typing import Any, Iterator, cast
from urllib.error import HTTPError
from datetime import datetime, timezone

from SPARQLWrapper import SPARQLWrapper, JSON as SPARQL_JSON

logger = logging.getLogger(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DEFAULT_ENDPOINT_URL = "https://sparql.knowledgehub.nfdi4earth.de/"

PAGE_SIZE = 50
MAX_RETRIES = 3

_NFDI4EARTH_CLIENT = SPARQLWrapper(DEFAULT_ENDPOINT_URL)
_NFDI4EARTH_CLIENT.setReturnFormat(SPARQL_JSON)

BASE_QUERY = """
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX locn: <http://www.w3.org/ns/locn#>

SELECT ?title ?authors ?description ?landingpage ?identifier ?download_urls
       ?startDate ?endDate ?publishers ?issued ?languages ?licenses ?keywords ?modified
WHERE {
  {
    SELECT DISTINCT ?dataset
    WHERE {
        ?dataset rdf:type dcat:Dataset;
                dct:title ?title.
    }
  }

  ?dataset dct:title ?title.

  @SINCE_FILTER@

  OPTIONAL {
    SELECT ?dataset (SAMPLE(?d) AS ?description)
    WHERE { ?dataset schema:description ?d. }
    GROUP BY ?dataset
  }

  OPTIONAL { ?dataset dcat:landingPage ?landingpage. }
  OPTIONAL { ?dataset dct:identifier ?identifier. }
  OPTIONAL { ?dataset dct:issued ?issued. }
  OPTIONAL { ?dataset dct:modified ?modified. }

  FILTER(BOUND(?issued) || BOUND(?modified))

  OPTIONAL {
    SELECT ?dataset (GROUP_CONCAT(DISTINCT ?authorName; separator=", ") AS ?authors)
    WHERE {
      ?dataset dct:creator ?bnode_creator.
      ?bnode_creator schema:name ?authorName.
    }
    GROUP BY ?dataset
  }

  OPTIONAL {
    SELECT ?dataset (GROUP_CONCAT(DISTINCT ?download_url; separator=", ") AS ?download_urls)
    WHERE {
      ?dataset dcat:distribution ?bnode_distrib.
      ?bnode_distrib dcat:downloadURL ?download_url.
    }
    GROUP BY ?dataset
  }

  OPTIONAL {
    SELECT ?dataset (SAMPLE(?sd) AS ?startDate) (SAMPLE(?ed) AS ?endDate)
    WHERE {
      ?dataset dct:temporal ?bnode_temporal.
      ?bnode_temporal dcat:startDate ?sd;
                       dcat:endDate ?ed.
    }
    GROUP BY ?dataset
  }


  OPTIONAL {
    SELECT ?dataset (GROUP_CONCAT(DISTINCT ?pubLabel; separator=", ") AS ?publishers)
    WHERE {
        ?dataset dct:publisher ?pub.
        OPTIONAL { ?pub foaf:name ?foafName. }
        OPTIONAL { ?pub schema:name ?schemaName. }
        BIND(
        IF(isBlank(?pub),
            COALESCE(?foafName, ?schemaName),
            STR(?pub))
        AS ?pubLabel
        )
    }
    GROUP BY ?dataset
    }

  OPTIONAL {
    SELECT ?dataset (GROUP_CONCAT(DISTINCT ?lang; separator=", ") AS ?languages)
    WHERE { ?dataset dct:language ?lang. }
    GROUP BY ?dataset
  }

  OPTIONAL {
    SELECT ?dataset (GROUP_CONCAT(DISTINCT ?lic; separator=", ") AS ?licenses)
    WHERE { ?dataset schema:license ?lic. }
    GROUP BY ?dataset
  }

  OPTIONAL {
    SELECT ?dataset (GROUP_CONCAT(DISTINCT ?kw; separator=", ") AS ?keywords)
    WHERE { ?dataset dcat:keyword ?kw. }
    GROUP BY ?dataset
  }
}
"""


def search_page(offset: int, since_filter: str = "") -> list[dict[str, Any]]:
    """
    Fetch one page of results from the NFDI4Earth KnowledgeHub SPARQL endpoint.

    Queries the endpoint with the module-level `BASE_QUERY`, using the
    module-level `PAGE_SIZE` as the row count, and retries on transient
    HTTP 504 timeouts.

    :param offset: The row offset to start the page at.
    :param since_filter: A SPARQL `FILTER(...)` clause restricting results
            to datasets modified within a date range, or `""` for no filter.

    :return: The list of SPARQL result bindings for that page.
    """
    query = BASE_QUERY.replace("@SINCE_FILTER@", since_filter)
    query += f"\nOFFSET {offset} LIMIT {PAGE_SIZE}"
    _NFDI4EARTH_CLIENT.setQuery(query)

    for attempt in range(MAX_RETRIES):
        try:
            response = _NFDI4EARTH_CLIENT.queryAndConvert()
            break
        except HTTPError as e:
            if e.code == 504 and attempt < MAX_RETRIES - 1:
                wait = 5 * (attempt + 1)
                logger.warning("504 timeout at offset %d, retrying in %ds...", offset, wait)
                time.sleep(wait)
            else:
                raise

    return cast(list[dict[str, Any]], response["results"]["bindings"])


def search_all() -> Iterator[list[dict[str, Any]]]:
    """
    Walk every page of the NFDI4Earth KnowledgeHub SPARQL endpoint and
    yield each page's bindings as they're fetched.

    :return: An iterator over pages, where each page is a list of SPARQL
            result bindings.
    """
    offset = 0
    total_records = 0
    page_num = 1
    while total_records < 15:
        page = search_page(offset)

        if not page:
            logger.info("Page %d empty, stopping", page_num)
            break

        total_records += len(page)
        logger.info(
            "Page %d: %d records (%d total so far)",
            page_num, len(page), total_records,
        )
        yield page

        if len(page) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        page_num += 1
        time.sleep(0.5)


def search_incremental(from_date: str, until_date: str) -> Iterator[list[dict[str, Any]]]:
    """
    Walk every page of the NFDI4Earth KnowledgeHub SPARQL endpoint for
    datasets modified within a date range, and yield each page's bindings
    as they're fetched.

    :param from_date: The start of the update-date range (ISO 8601).
    :param until_date: The end of the update-date range (ISO 8601).
    :return: An iterator over pages, where each page is a list of SPARQL
            result bindings.
    """
    since_filter = (
        f'FILTER('
        f'(BOUND(?modified) && ?modified >= "{from_date}"^^<http://www.w3.org/2001/XMLSchema#dateTime> && ?modified <= "{until_date}"^^<http://www.w3.org/2001/XMLSchema#dateTime>) '
        f'|| (!BOUND(?modified) && BOUND(?issued) && ?issued >= "{from_date}"^^<http://www.w3.org/2001/XMLSchema#dateTime> && ?issued <= "{until_date}"^^<http://www.w3.org/2001/XMLSchema#dateTime>)'
        f')'
    )

    offset = 0
    total_records = 0
    page_num = 1
    while total_records < 15:
        page = search_page(offset, since_filter)

        if not page:
            logger.info("Page %d empty, stopping", page_num)
            break

        total_records += len(page)
        logger.info(
            "Page %d: %d records (%d total so far)",
            page_num, len(page), total_records,
        )
        yield page

        if len(page) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        page_num += 1
        time.sleep(0.5)


def _binding_value(record: dict[str, Any], key: str) -> str | None:
    """Return the plain string value of a SPARQL binding, or None."""
    binding = record.get(key)
    return binding.get("value") if binding else None


def _split_list(value: str | None, sep: str = ",") -> list[str]:
    """Split a GROUP_CONCAT-style string into a clean list of parts."""
    if not value:
        return []
    return [p.strip() for p in value.split(sep) if p.strip()]


def _extract_doi(url: str | None) -> str | None:
    """Pull a bare DOI out of a URL, if present."""
    if not url:
        return None
    match = re.search(r"10\.\d{4,9}/\S+", url)
    return match.group(0) if match else None


_ROR_CACHE: dict[str, dict[str, Any] | None] = {}

def _resolve_ror_publisher(publisher_url: str) -> dict[str, Any] | None:
    """
    Resolve a Cordra publisher URL (e.g. .../objects/n4e/ror-032e6b942) to
    a name + ROR ID by extracting the ROR ID and querying the ROR API.

    Name preference order: English alias/label > ROR's own ror_display
    name (whatever language) > any remaining name. Acronyms are skipped
    since they're too terse to stand alone as a publisher name.

    Results are cached in-process by ROR ID since the same handful of
    organizations repeat across many records.

    :param publisher_url: The raw publisher value from the SPARQL result.
    :return: {"name": str, "ror_id": str} or None if it can't be resolved.
    """
    match = re.search(r"ror-([a-z0-9]+)", publisher_url)
    if not match:
        return None
    ror_id = match.group(1)

    if ror_id in _ROR_CACHE:
        return _ROR_CACHE[ror_id]

    result = None
    try:
        resp = requests.get(f"https://api.ror.org/organizations/{ror_id}", timeout=5)
        if resp.ok:
            data = resp.json()
            names = data.get("names", [])

            def pick(pred):
                for n in names:
                    if pred(n):
                        return n.get("value")
                return None

            name = (
                pick(lambda n: n.get("lang") == "en" and "acronym" not in n.get("types", []))
                or pick(lambda n: "ror_display" in n.get("types", []))
                or pick(lambda n: "acronym" not in n.get("types", []))
            )

            if name:
                result = {"name": name, "ror_id": ror_id}
    except requests.RequestException as e:
        logger.warning("ROR lookup failed for %s: %s", ror_id, e)

    _ROR_CACHE[ror_id] = result
    return result


def nfdi4earth_data_to_datacite(record: dict[str, Any]) -> tuple[str, str]:
    """
    Convert an NFDI4Earth KnowledgeHub dataset record into a DataCite 4.6
    XML record wrapped in an OAI-PMH <record> element.

    :param record: A single SPARQL result binding, as returned by
            `search_page`.

    :return: A tuple containing:
            - xml_pretty (str): Formatted DataCite XML record.
            - datestamp (str): Record update/creation date (YYYY-MM-DD).
    """
    title = _binding_value(record, "title")
    authors_raw = _binding_value(record, "authors")
    description = _binding_value(record, "description")
    landingpage = _binding_value(record, "landingpage")
    identifier = _binding_value(record, "identifier")
    download_urls_raw = _binding_value(record, "download_urls")
    start_date = _binding_value(record, "startDate")
    end_date = _binding_value(record, "endDate")
    publishers_raw = _binding_value(record, "publishers")
    issued = _binding_value(record, "issued")
    licenses_raw = _binding_value(record, "licenses")
    keywords_raw = _binding_value(record, "keywords")
    modified = _binding_value(record, "modified")

    doi = _extract_doi(identifier) or _extract_doi(landingpage)
    record_identifier = doi or landingpage or identifier or ""

    ET.register_namespace("", "http://www.openarchives.org/OAI/2.0/")
    ET.register_namespace("xsi", "http://www.w3.org/2001/XMLSchema-instance")

    # OAI-PMH RECORD ROOT
    oai_record = ET.Element(
        "record", {
            "xmlns": "http://www.openarchives.org/OAI/2.0/",
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        })

    header = ET.SubElement(oai_record, "header")
    ET.SubElement(header, "identifier").text = record_identifier

    datestamp_text = (modified or issued or "")[:10]

    metadata = ET.SubElement(oai_record, "metadata")

    resource = ET.SubElement(
        metadata,
        "resource",
        {
            "xmlns": "http://datacite.org/schema/kernel-4",
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xsi:schemaLocation": (
                "http://datacite.org/schema/kernel-4 "
                "https://schema.datacite.org/meta/kernel-4.6/metadata.xsd"
            ),
        },
    )

    # IDENTIFIER (mandatory)
    if doi:
        ET.SubElement(resource, "identifier", identifierType="DOI").text = doi
    else:
        ET.SubElement(resource, "identifier", identifierType="URL").text = landingpage or ""

    # CREATORS
    author_names = _split_list(authors_raw, sep=",")
    if author_names:
        creators = ET.SubElement(resource, "creators")
        for name in author_names:
            creator = ET.SubElement(creators, "creator")
            ET.SubElement(creator, "creatorName").text = name  # no nameType attribute at all

    # TITLES (mandatory)
    titles = ET.SubElement(resource, "titles")
    ET.SubElement(titles, "title").text = title

    # PUBLISHER (mandatory)
    publisher_list = _split_list(publishers_raw, sep=",")
    publisher_el = ET.SubElement(resource, "publisher")
    if publisher_list:
        resolved = _resolve_ror_publisher(publisher_list[0])
        if resolved:
            publisher_el.text = resolved["name"]
            publisher_el.set("publisherIdentifier", f"https://ror.org/{resolved['ror_id']}")
            publisher_el.set("publisherIdentifierScheme", "ROR")
            publisher_el.set("schemeURI", "https://ror.org")
        else:
            publisher_el.text = publisher_list[0]  # fall back to raw URL if resolution fails
    else:
        publisher_el.text = "unknown"

    # PUBLICATION YEAR (mandatory)
    pub_year_source = issued or modified
    if pub_year_source:
        year_match = re.search(r"(\d{4})", pub_year_source)
        if year_match:
            ET.SubElement(resource, "publicationYear").text = year_match.group(1)

    # RESOURCE TYPE (mandatory)
    ET.SubElement(resource, "resourceType", resourceTypeGeneral="Dataset").text = "Dataset"

    # DATES
    if issued or start_date or end_date or modified:
        dates = ET.SubElement(resource, "dates")
        if issued:
            ET.SubElement(dates, "date", dateType="Issued").text = issued
        if start_date and end_date:
            ET.SubElement(dates, "date", dateType="Collected").text = f"{start_date}/{end_date}"
        elif start_date:
            ET.SubElement(dates, "date", dateType="Collected").text = start_date
        if modified:
            ET.SubElement(dates, "date", dateType="Updated").text = modified

    # RELATED IDENTIFIERS
    # DataCite has no dedicated "download URL" field; recording these as
    # relatedIdentifiers of type URL.
    download_urls = _split_list(download_urls_raw, sep=",")
    if download_urls:
        related = ET.SubElement(resource, "relatedIdentifiers")
        for url in download_urls:
            ET.SubElement(related, "relatedIdentifier", relatedIdentifierType="URL", relationType="IsSourceOf").text = url

    # SUBJECTS
    keywords = _split_list(keywords_raw, sep=",")
    if keywords:
        subjects_el = ET.SubElement(resource, "subjects")
        for word in keywords:
            ET.SubElement(subjects_el, "subject").text = word

    # RIGHTS
    licenses = _split_list(licenses_raw, sep=",")
    if licenses:
        rights_list = ET.SubElement(resource, "rightsList")
        for lic in licenses:
            ET.SubElement(rights_list, "rights", rightsURI=lic)

    # DESCRIPTIONS
    if description:
        descriptions = ET.SubElement(resource, "descriptions")
        ET.SubElement(descriptions, "description", descriptionType="Abstract").text = description

    xml_str = ET.tostring(oai_record, encoding="unicode")
    xml_pretty = minidom.parseString(xml_str).toprettyxml(indent="  ")

    return xml_pretty, datestamp_text


def run_harvester_nfdi4earth(run_info: dict[str, Any]) -> bool:
    """
    Run a full (or incremental) NFDI4Earth KnowledgeHub harvest and push
    each entry to the data warehouse as a harvest event.

    :param run_info: Dictionary describing the harvest run.

    :return: `True` if every harvest event was sent successfully (and no
            unexpected exception occurred), `False` if any event failed to
            send or an exception was raised during the run.
    """
    record_count = 0
    harvest_events = 0
    failed_events = 0
    try:

        config = run_info.get("endpoint_config")
        if config is None:
            raise ValueError("config is missing")
        harvest_url = config.get("harvest_url")
        from_date = run_info.get("from_date")
        until_date = run_info.get("until_date")
        if until_date is None:
            raise ValueError("Missing until_date parameter")
        pages = search_all() if not from_date else search_incremental(from_date, until_date)
        for page in pages:
            for record in page:
                xml_out, datestamp = nfdi4earth_data_to_datacite(record)
                record_identifier = _extract_doi(_binding_value(record, "identifier")) \
                    or _extract_doi(_binding_value(record, "landingpage")) \
                    or _binding_value(record, "landingpage") \
                    or _binding_value(record, "identifier") \
                    or ""
                record_count += 1

                event_payload = {
                    "record_identifier": record_identifier,
                    "datestamp": datestamp,
                    "raw_metadata": xml_out,
                    "additional_metadata": json.dumps({}),
                    "harvest_url": harvest_url,
                    "repo_code": config.get("code"),
                    "harvest_run_id": run_info.get("id"),
                    "is_deleted": False,
                }

                if send_harvest_event(event_payload):
                    harvest_events += 1
                else:
                    failed_events += 1

        logger.info(
            "Harvest summary: processed %s records, successfully sent %s of them to the warehouse, "
            "failed to send %s records.",
            record_count,
            harvest_events,
            failed_events
        )

        return failed_events == 0

    except Exception as e:
        logger.exception("Unexpected error in run_harvester_nfdi4earth: %s", e)
        logger.info(
            "Harvest summary: processed %s records, successfully sent %s of them to the warehouse, "
            "failed to send %s records.",
            record_count,
            harvest_events,
            failed_events        )
        return False

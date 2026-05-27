import httpx
from httpx_retries import Retry, RetryTransport
import asyncio
import logging
import json
import os
from datetime import datetime, timezone
from lxml import etree
from harvester.settings import settings
from harvester.db_api_functions import send_harvest_event
from urllib.parse import quote
import re

ACCESS_TOKEN = settings.FINBIF_ACCESS_TOKEN

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Base URL and config for the FinBIF API
API_BASE = "https://api.gbif.org"
API_ADDITIONAL = "https://tun.fi"
KEY = "b1304814-56cc-434e-8d40-2b24fa21526f"

OAI_NS = "http://www.openarchives.org/OAI/2.0/"
DC_NS = "http://datacite.org/schema/kernel-4"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
SCHEMA_LOCATION = "http://datacite.org/schema/kernel-4 https://schema.datacite.org/meta/kernel-4.5/metadata.xsd"
XML_NS = "http://www.w3.org/XML/1998/namespace"

_HTML_TAG_RE = re.compile(r"<[^>]+>")

_RIGHTS_MAP = {
    "MY.intellectualRightsCC-BY": (
        "http://creativecommons.org/licenses/by/4.0/",
        "CC-BY-4.0",
        "info:eu-repo/semantics/openAccess",
    ),
    "MY.intellectualRightsCC0": (
        "http://creativecommons.org/publicdomain/zero/1.0/",
        "CC0-1.0",
        "info:eu-repo/semantics/openAccess",
    ),
}

retry_strategy = Retry(
    total=8,  # Number of retries
    backoff_factor=0.5,  # Delay between retries (exponential backoff)
)
_FINBIF_CLIENT = httpx.Client(
    transport=RetryTransport(retry=retry_strategy),
    timeout=httpx.Timeout(120),
    #headers={"Authorization": f"Bearer {ACCESS_TOKEN}"}
)
_ASYNC_FINBIF_CLIENT = httpx.AsyncClient(
    #headers={"Authorization": f"Bearer {ACCESS_TOKEN}"},
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


def build_datacite_xml(record: dict) -> str:
    dataset = record["dataset"]
    additional = record["additional"]

    # OAI-PMH wrapper
    root = etree.Element(
        f"{{{OAI_NS}}}record",
        nsmap={None: OAI_NS, "xsi": XSI_NS},
    )

    # Header
    header = etree.SubElement(root, f"{{{OAI_NS}}}header")
    identifier_el = etree.SubElement(header, f"{{{OAI_NS}}}identifier")
    identifier_el.text = f"doi:{dataset['doi']}"
    datestamp_el = etree.SubElement(header, f"{{{OAI_NS}}}datestamp")
    datestamp_el.text = datetime.fromisoformat(dataset["modified"]).date().isoformat()

    # Metadata
    metadata = etree.SubElement(root, f"{{{OAI_NS}}}metadata")

    # DataCite resource
    resource = etree.SubElement(
        metadata,
        "resource",
        nsmap={None: DC_NS, "xsi": XSI_NS},
        attrib={f"{{{XSI_NS}}}schemaLocation": SCHEMA_LOCATION},
    )

    # identifier
    etree.SubElement(resource, "identifier", identifierType="DOI").text = dataset["doi"]

    # creators
    creators_el = etree.SubElement(resource, "creators")
    creator_el = etree.SubElement(creators_el, "creator")
    etree.SubElement(creator_el, "creatorName", nameType="Organizational").text = additional["intellectualOwner"]

    # titles
    titles_el = etree.SubElement(resource, "titles")
    etree.SubElement(titles_el, "title").text = dataset["title"]

    # subjects
    subjects_el = etree.SubElement(resource, "subjects")

    # taxonomicCoverage -> split on comma
    if "taxonomicCoverage" in additional:
        for taxon in additional["taxonomicCoverage"].split(","):
            etree.SubElement(subjects_el, "subject").text = taxon.strip()

    # geographicCoverage -> split on comma
    if "geographicCoverage" in additional:
        for place in additional["geographicCoverage"].split(","):
            etree.SubElement(subjects_el, "subject").text = place.strip()

    # coverageBasis -> plain subject
    if "coverageBasis" in additional:
        etree.SubElement(subjects_el, "subject").text = additional["coverageBasis"]

    # longNameMultiLang -> subjects with lang
    for lang, text in additional.get("longNameMultiLang", {}).items():
        etree.SubElement(subjects_el, "subject",
                         attrib={f"{{{XML_NS}}}lang": lang},
                         ).text = text

    # contributors
    if dataset["contacts"]:
        contributors_el = etree.SubElement(resource, "contributors")
        contact = dataset["contacts"][0]
        contributor_el = etree.SubElement(contributors_el, "contributor", contributorType="ContactPerson")
        given = contact.get("firstName", "")
        family = contact.get("lastName", "")
        etree.SubElement(contributor_el, "contributorName", nameType="Personal").text = f"{family}, {given}".strip(", ")
        if given:
            etree.SubElement(contributor_el, "givenName").text = given
        if family:
            etree.SubElement(contributor_el, "familyName").text = family

    # dates
    dates_el = etree.SubElement(resource, "dates")
    etree.SubElement(dates_el, "date", dateType="Created").text = datetime.fromisoformat(
        dataset["created"]).date().isoformat()
    etree.SubElement(dates_el, "date", dateType="Updated").text = datetime.fromisoformat(
        dataset["modified"]).date().isoformat()

    # publicationYear
    etree.SubElement(resource, "publicationYear").text = dataset["created"][:4]

    etree.SubElement(resource, "publisher").text = additional.get("publisherShortname", additional["intellectualOwner"])

    # resourceType
    etree.SubElement(resource, "resourceType", resourceTypeGeneral="Dataset")

    # descriptions
    descriptions_el = etree.SubElement(resource, "descriptions")
    description_el = etree.SubElement(descriptions_el, "description", descriptionType="Abstract")
    description_el.text = _HTML_TAG_RE.sub("", dataset["description"]).strip()

    # multilang descriptions
    for lang, text in additional.get("descriptionMultiLang", {}).items():
        el = etree.SubElement(descriptions_el, "description",
                              descriptionType="Abstract",
                              attrib={f"{{{XML_NS}}}lang": lang},
                              )
        el.text = text

    # rightsList
    intellectual_rights = additional.get("intellectualRights")
    if intellectual_rights in _RIGHTS_MAP:
        rights_uri, rights_label, access_uri = _RIGHTS_MAP[intellectual_rights]
        rights_list_el = etree.SubElement(resource, "rightsList")
        etree.SubElement(rights_list_el, "rights", rightsURI=access_uri)
        etree.SubElement(rights_list_el, "rights", rightsURI=rights_uri).text = rights_label

    return etree.tostring(root, pretty_print=True, xml_declaration=True, encoding="UTF-8").decode()

def harvest_datasets(from_date: datetime | None) -> list[dict]:
    logger.info(f'Getting datasets with from_date: {from_date}')

    response = _FINBIF_CLIENT.get(f'{API_BASE}/v1/installation/{KEY}/dataset', params={"limit": 1000}, headers={"Accept": "application/json", "User-Agent": "EOSC Data Commons harvester"})
    response.raise_for_status()
    data = response.json()
    datasets =  data["results"]

    if from_date is not None:
        return [
            d for d in datasets
            if datetime.fromisoformat(d["modified"]) > from_date
        ]
    return datasets

async def harvest_finbif(run_info: dict) -> bool:
    harvest_events = 0
    failed_events = 0
    record_counter = 0
    success = True

    harvest_run_id = run_info.get("id")
    from_date = run_info.get("from_date")
    from_ = datetime.fromisoformat(from_date.replace("Z", "+00:00")) if from_date else None

    logger.info(run_info)

    if from_:
        logger.info("Incremental harvest since %s", from_date)
    else:
        logger.info("First harvest, fetching all records.")

    combined = []
    try:
        datasets = harvest_datasets(from_)

        dwc_urls = [
            ep["url"]
            for obj in datasets
            for ep in obj["endpoints"]
            if ep["type"] == "DWC_ARCHIVE"
        ]

        ids = [url.split('/')[-1].removesuffix('.zip') for url in dwc_urls]

        results = await asyncio.gather(*[
            _ASYNC_FINBIF_CLIENT.get(f'{API_ADDITIONAL}/{id_}', params={"format": "json"})
            for id_ in ids
        ])

        additional_data = []
        for response in results:
            response.raise_for_status()
            additional_data.append(response.json())

        for dataset, additional in zip(datasets, additional_data):
            combined.append({"dataset": dataset, "additional": additional})

        with open("finbif.json", "w") as f:
            f.write(json.dumps(combined, indent=4))

    except httpx.RequestError as e:
        logger.error("Network error while fetching collections: %s", e)
        return False
    except httpx.HTTPStatusError as e:
        logger.error("HTTP error while fetching collections: %s", e)
        return False
    except Exception as e:
        logger.error(f"Unexpected error while harvesting datasets: {e}")
        return False

    finally:
        shutdown_client()
        await shutdown_async_client()

    for record in combined:
        record_counter += 1
        record_identifier = record["dataset"]["doi"]

        datacite_xml = build_datacite_xml(record)

        with open(f"finbif/{quote(record_identifier, safe="")}", "w") as f:
            f.write(datacite_xml)

        try:
            event_payload = {
                "record_identifier": record_identifier,
                "datestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "raw_metadata": datacite_xml,
                "additional_metadata": json.dumps(record),
                "harvest_url": "https://api.laji.fi",
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

def run_harvester_finbif(run_info: dict) -> bool:
    """
    Entry point for FinBIF harvesting from main.py
    """
    try:
        return asyncio.run(harvest_finbif(run_info))
    except Exception as e:
        logger.exception("FinBIF harvester crashed: %s", e)
        return False

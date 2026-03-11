import json
import logging
from datetime import datetime, timezone
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from rdflib import Dataset, Namespace
from rdflib.namespace import RDF
from rdflib.term import Node

from harvester.datacite_model import (
    Creator,
    DataciteRecord2,
    Date,
    Description,
    Subject,
    Title,
    Types,
)
from harvester.db_api_functions import send_harvest_event
from harvester.models import HarvestEventCreateRequest, HarvestRunCreateResponse

logger = logging.getLogger(__name__)

# Support both https and http schema.org namespace variants
_SDO = (Namespace("https://schema.org/"), Namespace("http://schema.org/"))


def _vals(g: Dataset, subject: Node, prop: str) -> list[str]:
    """All string values for schema:prop on subject, deduplicated across both SDO namespaces."""
    # use a single ordered mapping to deduplicate while preserving order
    seen: dict[str, None] = {}
    for ns in _SDO:
        for obj in g.objects(subject, ns[prop]):
            seen[str(obj)] = None
    return list(seen.keys())


def _val(g: Dataset, subject: Node, prop: str) -> str | None:
    """First string value for schema:prop on subject."""
    vals = _vals(g, subject, prop)
    return vals[0] if vals else None


def _is_dataset(g: Dataset, node: Node) -> bool:
    return any((node, RDF.type, ns["Dataset"]) in g for ns in _SDO)


def _top_level_datasets(g: Dataset) -> list[Node]:
    """Dataset nodes that are not hasPart children of another Dataset."""
    all_ds: set[Node] = {s for ns in _SDO for s in g.subjects(RDF.type, ns["Dataset"])}
    children: set[Node] = {
        child
        for ns in _SDO
        for parent in all_ds
        for child in g.objects(parent, ns["hasPart"])
        if child in all_ds
    }
    return [ds for ds in all_ds if ds not in children]


def _build_datacite_record(g: Dataset, node: Node) -> DataciteRecord2:
    """Build a DataciteRecord2 from a schema:Dataset node in the RDF graph."""
    # Use schema:url if available, otherwise fall back to the subject @id
    url = _val(g, node, "url") or str(node)
    # If the URL itself is a DOI, extract it
    doi: str | None = url if url.startswith(("https://doi.org/", "http://doi.org/")) else None
    name = _val(g, node, "name") or ""

    # Creators
    seen_creators: set[str] = set()
    creators: list[Creator] = []
    for ns in _SDO:
        for creator_node in g.objects(node, ns["creator"]):
            cname = _val(g, creator_node, "name") or "Unknown"
            if cname not in seen_creators:
                seen_creators.add(cname)
                creators.append(Creator(creatorName=cname, nameType="Organizational"))
    if not creators:
        creators = [Creator(creatorName="Unknown")]

    # Dates
    date_modified = _val(g, node, "dateModified")
    pub_year = date_modified[:4] if date_modified else None
    dates = [Date(date=date_modified, dateType="Updated")] if date_modified else None

    # Description: parent + hasPart child descriptions as bullet points
    desc_parts: list[str] = []
    parent_desc = _val(g, node, "description")
    if parent_desc:
        desc_parts.append(parent_desc)
    seen_parts: set[Node] = set()
    for ns in _SDO:
        for part in g.objects(node, ns["hasPart"]):
            if part in seen_parts or not _is_dataset(g, part):
                continue
            seen_parts.add(part)
            part_name = _val(g, part, "name")
            part_desc = _val(g, part, "description")
            if part_desc:
                bullet = f"- **{part_name}**: {part_desc}" if part_name else f"- {part_desc}"
                desc_parts.append(bullet)
    full_desc = "\n".join(desc_parts) if desc_parts else None
    descriptions = [Description(description=full_desc, descriptionType="Abstract")] if full_desc else None

    # Keywords → subjects
    subjects = [Subject(subject=kw) for kw in _vals(g, node, "keywords") if kw] or None

    # # DOI from citation (literal may be a JSON-encoded array string or a plain URL)
    # doi: str | None = None
    # for citation in _vals(g, node, "citation"):
    #     candidates: list[str]
    #     try:
    #         parsed = json.loads(citation)
    #         candidates = parsed if isinstance(parsed, list) else [citation]
    #     except (json.JSONDecodeError, TypeError):
    #         candidates = [citation]
    #     for c in candidates:
    #         if "doi.org" in c:
    #             doi = c
    #             break
    #     if doi:
    #         break

    return DataciteRecord2(
        id=url,
        doi=doi,
        url=url,
        titles=[Title(title=name)],
        creators=creators,
        publicationYear=pub_year,
        descriptions=descriptions,
        dates=dates,
        subjects=subjects,
        types=Types(resourceTypeGeneral="Dataset"),
    )


def _process_page(soup: BeautifulSoup, page_url: str, run_info: HarvestRunCreateResponse) -> int:
    """
    Parse all JSON-LD blocks on a page, build DataciteRecord2 for each Dataset found,
    send a HarvestEventCreateRequest per record
    """
    scripts = soup.find_all("script", type="application/ld+json")
    ds_count = 0
    for script in scripts:
        raw = script.string
        if not raw:
            continue

        g = Dataset()
        g.parse(data=raw, format="json-ld")
        top_datasets = _top_level_datasets(g)
        logger.info(f"Found {len(top_datasets)} top-level dataset(s) in JSON-LD block on {page_url}")

        for ds in top_datasets:
            record = _build_datacite_record(g, ds)
            # logger.info(json.dumps(record.model_dump(mode="json"), indent=2))
            event = HarvestEventCreateRequest(
                record_identifier=record.url,
                datestamp=datetime.now(timezone.utc),
                raw_metadata=raw,
                additional_metadata=json.dumps(record.model_dump(mode="json")),
                harvest_url=page_url,
                repo_code=run_info.endpoint_config.code,
                harvest_run_id=run_info.id,
                is_deleted=False,
            )
            success = send_harvest_event(event.model_dump(mode="json"))
            logger.info("Sent harvest event for %s: %s", record.url, "OK" if success else "FAILED")
            ds_count += 1
    return ds_count

def run_harvester_schemaorg(run_info: HarvestRunCreateResponse) -> bool:
    """
    Entry point for Schema.org harvesting from main.py.

    1. Fetches `harvest_url` and extracts JSON-LD from it directly.
    2. Also follows any `<a data-discover="true">` links found on that page
        and extracts JSON-LD from each of those pages too.
    """
    harvest_url = run_info.endpoint_config.harvest_url
    ds_count = 0

    try:
        with httpx.Client(timeout=httpx.Timeout(30)) as client:
            resp = client.get(harvest_url)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")
            ds_count += _process_page(soup, harvest_url, run_info)

            # Go down discoverable links like `<a href="/species/9606" data-discover="true">`
            linked_urls = [
                urljoin(harvest_url, str(a["href"]))
                for a in soup.find_all("a", attrs={"data-discover": "true", "href": True})
            ]
            logger.info(f"Found {len(linked_urls)} data-discover pages linked from {harvest_url}")

            for url in linked_urls:
                page_resp = client.get(url)
                page_resp.raise_for_status()
                page_soup = BeautifulSoup(page_resp.text, "lxml")
                ds_count += _process_page(page_soup, url, run_info)
            logger.info(f"Total datasets found: {ds_count}")
            return True

    except Exception as e:
        logger.exception(f"Schema.org harvester crashed: {e}")
        return False


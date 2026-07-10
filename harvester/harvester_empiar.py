import logging, os, httpx, xml.etree.ElementTree as ET, time, mimetypes, json
from datetime import datetime
from .db_api_functions import send_harvest_event
from xml.dom import minidom
from typing import Any, Dict, Iterator, Optional

_MDPOSIT_CLIENT = httpx.Client(
    timeout = httpx.Timeout(120),
)

logger = logging.getLogger(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DEFAULT_BASE_URL = "https://www.ebi.ac.uk"
SEARCH_PATH = "/emdb/api/empiar/search/{query}"

PAGE_SIZE = 100



def search_page(page: int) -> Dict[str, Any]:
    """One page of `GET /emdb/api/empiar/search/{query}`."""
    url = DEFAULT_BASE_URL + SEARCH_PATH.format(query = "*")
    params: Dict[str, Any] = {"rows": PAGE_SIZE, "page": page, "fl": "*"}
    resp = _MDPOSIT_CLIENT.get(url, params = params)
    resp.raise_for_status()
    return resp.json()



def search_all() -> Iterator[Dict[str, Any]]:
    """
    Walk every page of the search endpoint for `query` and yield each
    page's payload (dict of empiar_id -> metadata) as it's fetched.
    """
    page = 1
    total_records = 0
    while total_records < 50:
        payload = search_page(page)

        if not payload:
            logger.info("Page %d empty, stopping", page)
            break

        total_records += len(payload)
        logger.info(
            "Page %d: %d records (%d total so far)",
            page, len(payload), total_records,
        )
        yield payload

        if len(payload) < PAGE_SIZE:
            # Last page was partial, nothing more to fetch.
            logger.info("Done: %d records across %d pages", total_records, page)
            break

        page += 1
        time.sleep(0.5)


EMPIAR_PUBLISHER = "Electron Microscopy Public Image Archive (EMPIAR)"
EMPIAR_LANDING_PAGE = "https://www.ebi.ac.uk/empiar/{entry_id}/"
ORCID_SCHEME_URI = "https://orcid.org"

 
 
def _person_display_name(person: Dict[str, Any]) -> str:
    first = (person or {}).get("first_name") or ""
    last = (person or {}).get("last_name") or ""
    full = f"{last}, {first}".strip(", ") if (first or last) else ""
    return full or "(:unav)"
 
 
def _add_name_identifier(parent_el: ET.Element, orcid: str | None) -> None:
    """Attach a DataCite <nameIdentifier> element for an ORCID iD, if present."""
    if not orcid:
        return
    orcid_clean = orcid.strip()
    if not orcid_clean:
        return
    name_id = ET.SubElement(
        parent_el,
        "nameIdentifier",
        nameIdentifierScheme="ORCID",
        schemeURI=ORCID_SCHEME_URI,
    )
    name_id.text = orcid_clean if orcid_clean.startswith("http") else f"{ORCID_SCHEME_URI}/{orcid_clean}"
 
 
def empiar_data_to_datacite(entry_id: str, record: Dict[str, Any]) -> tuple[str, str, str]:
    """
    Convert an EMPIAR entry record into a DataCite XML record.
 
    Args:
        entry_id: The numeric EMPIAR id (e.g. "12646"), without the
            "EMPIAR-" prefix.
        record: Entry dictionary obtained from the EMPIAR API, i.e. the
            value keyed under "EMPIAR-<entry_id>" in the API response.
 
    Returns:
        A tuple containing:
            - xml_pretty (str): Formatted DataCite XML record.
            - identifier (str): Dataset DOI/identifier text.
            - datestamp (str): Record update or creation date
              in YYYY-MM-DD format.
    """
 
    meta = record
 
    # NAMESPACES
    ET.register_namespace("", "http://www.openarchives.org/OAI/2.0/")
    ET.register_namespace("xsi", "http://www.w3.org/2001/XMLSchema-instance")
 
    # DATES
    creation_date = meta.get("deposition_date", "") or ""
    update_date = meta.get("update_date", "") or ""
    release_date = meta.get("release_date") or ""
    obsolete_date = meta.get("obsolete_date")
 
    # OAI-PMH RECORD ROOT
    oai_record = ET.Element(
        "record",
        {
            "xmlns": "http://www.openarchives.org/OAI/2.0/",
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        },
    )
 
    # HEADER
    header_attrs = {"status": "deleted"} if obsolete_date else {}
    header = ET.SubElement(oai_record, "header", header_attrs)
 
    header_identifier = ET.SubElement(header, "identifier")
    header_identifier.text = f"EMPIAR-{entry_id}"
 
    datestamp = ET.SubElement(header, "datestamp")
    datestamp.text = update_date[:10] if update_date else creation_date[:10]
 
    # METADATA BLOCK
    metadata = ET.SubElement(oai_record, "metadata")
 
    # DATACITE RESOURCE
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
 
    # IDENTIFIER (mandatory; identifierType is fixed to "DOI" in the schema)
    doi = meta.get("entry_doi") or ""
    doi_value = ""
    if doi:
        doi_value = doi.split("doi:")[-1] if doi.lower().startswith("doi:") else doi
    identifier = ET.SubElement(resource, "identifier", identifierType="DOI")
    identifier.text = doi_value or "(:unav)"
 
    # If there's no real DOI, still preserve the landing page as a related identifier
    if not doi_value:
        landing_page_url = EMPIAR_LANDING_PAGE.format(entry_id=entry_id)
    else:
        landing_page_url = None
 
    # CREATORS (mandatory)
    creators = ET.SubElement(resource, "creators")
    author_entries = []  # list of (name, orcid)
    for a in meta.get("authors") or []:
        author = (a or {}).get("author", {})
        name = author.get("name")
        if name:
            author_entries.append((name, author.get("author_orcid")))
    if not author_entries:
        for cit in meta.get("citation") or []:
            for a in cit.get("authors") or []:
                if a.get("name"):
                    author_entries.append((a["name"], a.get("author_orcid")))
            if author_entries:
                break
    if not author_entries:
        pis = meta.get("principal_investigator") or []
        for pi in pis:
            author_entries.append((_person_display_name(pi), pi.get("author_orcid")))
 
    for name, orcid in author_entries or [("(:unav)", None)]:
        creator = ET.SubElement(creators, "creator")
        creator_name = ET.SubElement(creator, "creatorName", nameType="Personal")
        creator_name.text = name
        _add_name_identifier(creator, orcid)
 
    # TITLES (mandatory)
    titles = ET.SubElement(resource, "titles")
    ET.SubElement(titles, "title").text = meta.get("title", "") or "(:unav)"
 
    # PUBLISHER (mandatory)
    ET.SubElement(resource, "publisher").text = EMPIAR_PUBLISHER
 
    # PUBLICATION YEAR (mandatory)
    pub_year_source = release_date or creation_date
    pub_year = pub_year_source[:4] if pub_year_source else ""
    ET.SubElement(resource, "publicationYear").text = pub_year or "(:unav)"
 
    # RESOURCE TYPE (mandatory)
    ET.SubElement(
        resource, "resourceType", resourceTypeGeneral="Dataset"
    ).text = meta.get("experiment_type") or "Electron Microscopy Image Data"
 
    # LANGUAGE (optional, recommended if available)
    languages = {
        cit.get("language") for cit in (meta.get("citation") or []) if cit.get("language")
    }
    if languages:
        # DataCite expects an IETF language tag; fall back to the raw value if
        # it isn't already one (e.g. "English" instead of "en").
        lang_map = {"english": "en"}
        raw_lang = sorted(languages)[0]
        ET.SubElement(resource, "language").text = lang_map.get(raw_lang.lower(), raw_lang)
 
    # CONTRIBUTORS
    pi_list = meta.get("principal_investigator") or []
    corr = (meta.get("corresponding_author") or {}).get("author")
    seen_names = set()
    contact_people = []
    for person in list(pi_list) + ([corr] if corr else []):
        if not person:
            continue
        name = _person_display_name(person)
        if name in seen_names:
            continue
        seen_names.add(name)
        contact_people.append(person)
 
    if contact_people:
        contributors = ET.SubElement(resource, "contributors")
        for person in contact_people:
            contributor = ET.SubElement(contributors, "contributor", contributorType="ContactPerson")
            contributor_name = ET.SubElement(contributor, "contributorName", nameType="Personal")
            contributor_name.text = _person_display_name(person)
            _add_name_identifier(contributor, person.get("author_orcid"))
            if person.get("organization"):
                ET.SubElement(contributor, "affiliation").text = person["organization"]
 
    # DATES
    dates = ET.SubElement(resource, "dates")
    if creation_date:
        ET.SubElement(dates, "date", dateType="Submitted").text = creation_date[:10]
    if release_date:
        ET.SubElement(dates, "date", dateType="Available").text = release_date[:10]
    if update_date:
        ET.SubElement(dates, "date", dateType="Updated").text = update_date[:10]
 
    # RELATED IDENTIFIERS
    related = ET.SubElement(resource, "relatedIdentifiers")
 
    if landing_page_url:
        ET.SubElement(
            related, "relatedIdentifier",
            relatedIdentifierType="URL", relationType="IsIdentifiedBy",
        ).text = landing_page_url
 
    # EMDB cross-references
    for xref in meta.get("cross_references") or []:
        emd_id = xref.get("name") if isinstance(xref, dict) else xref
        if emd_id:
            ET.SubElement(
                related, "relatedIdentifier",
                relatedIdentifierType="URL", relationType="IsDerivedFrom",
            ).text = f"https://www.ebi.ac.uk/emdb/{emd_id}"
 
    # Citation DOI / PubMed
    for cit in meta.get("citation") or []:
        if cit.get("doi"):
            cit_doi_value = cit["doi"].split("doi:")[-1] if cit["doi"].lower().startswith("doi:") else cit["doi"]
            ET.SubElement(
                related, "relatedIdentifier",
                relatedIdentifierType="DOI", relationType="IsDocumentedBy",
            ).text = cit_doi_value
        if cit.get("pubmedid"):
            ET.SubElement(
                related, "relatedIdentifier",
                relatedIdentifierType="PMID", relationType="IsDocumentedBy",
            ).text = cit["pubmedid"]
 
    # SIZES
    dataset_size = meta.get("dataset_size")
    if dataset_size:
        sizes_el = ET.SubElement(resource, "sizes")
        ET.SubElement(sizes_el, "size").text = dataset_size
 
    # SUBJECTS (image-set categories)
    categories = {
        imgset.get("category")
        for imgset in meta.get("imagesets") or []
        if imgset.get("category")
    }
    if categories:
        subjects_el = ET.SubElement(resource, "subjects")
        for cat in sorted(categories):
            ET.SubElement(subjects_el, "subject").text = cat
 
    # FORMATS
    formats_set = {
        imgset[key]
        for imgset in meta.get("imagesets") or []
        for key in ("header_format", "data_format")
        if imgset.get(key)
    }
    if formats_set:
        formats_el = ET.SubElement(resource, "formats")
        for fmt in sorted(formats_set):
            ET.SubElement(formats_el, "format").text = fmt
 
    # RIGHTS
    rights_list = ET.SubElement(resource, "rightsList")
    ET.SubElement(
        rights_list, "rights",
        rightsURI="https://creativecommons.org/publicdomain/zero/1.0/",
        rightsIdentifier="CC0-1.0",
    ).text = "CC0 1.0 Universal (CC0 1.0) Public Domain Dedication"
 
    # DESCRIPTIONS
    descriptions = ET.SubElement(resource, "descriptions")
    if meta.get("title"):
        ET.SubElement(descriptions, "description", descriptionType="Abstract").text = meta["title"]
    for imgset in meta.get("imagesets") or []:
        details = imgset.get("details")
        if details:
            ET.SubElement(
                descriptions, "description", descriptionType="TechnicalInfo",
            ).text = f"{imgset.get('name', 'imageset')}: {details}"
 
    # FUNDING REFERENCES
    grants = meta.get("grant_references") or []
    if grants:
        funding_refs = ET.SubElement(resource, "fundingReferences")
        for grant in grants:
            funding_ref = ET.SubElement(funding_refs, "fundingReference")
            funder_name = grant.get("funding_body") or "(:unav)"
            ET.SubElement(funding_ref, "funderName").text = funder_name
            if grant.get("code"):
                ET.SubElement(funding_ref, "awardNumber").text = grant["code"]
 
    # VERSION
    history = meta.get("version_history") or []
    if history:
        latest = history[-1]
        version_value = latest.get("version") if isinstance(latest, dict) else str(latest)
        if version_value:
            ET.SubElement(resource, "version").text = version_value
 
    xml_str = ET.tostring(oai_record, encoding="unicode")
    xml_pretty = minidom.parseString(xml_str).toprettyxml(indent="  ")
    return xml_pretty, identifier.text, (update_date or creation_date)[:10]



def run_harvester_empiar(run_info: dict) -> bool:
    """
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
        headers = {"Accept": "application/json"}
        all_projects = {}
        if not from_date:
            projects = list(search_all())

        # else:
        #     projects = search_incremental(config)
        

        projects = list(search_all())

        xml_records = []
        for page in projects:
            for empiar_key, record in page.items():
                entry_id = empiar_key.removeprefix("EMPIAR-")  # "EMPIAR-12646" -> "12646"
                xml_out, identifier, datestamp = empiar_data_to_datacite(entry_id, record)
                print(xml_out)
                # xml_records.append(xml_out)

        # for empiar_id, metadata in projects.items():
        #     empiar_xml, identifier, datestamp = empiar_data_to_datacite(metadata)
        #     print(empiar_xml)

        #     event_payload = {
        #         "record_identifier": empiar_id,
        #         "datestamp": datestamp,
        #         "raw_metadata": empiar_xml,
        #         # "additional_metadata": additional_file_metadata,
        #         "harvest_url": harvest_url,
        #         "repo_code": "MDDB",
        #         "harvest_run_id": run_info.get("id"),
        #         "is_deleted": False
        #     }

        #     if send_harvest_event(event_payload):
        #         harvest_events += 1
        #     else:
        #         failed_events += 1

        # logger.info(
        #     "Harvest summary: processed %s records, successfully sent %s of them to the warehouse, failed to send %s records.",
        #     record_count,
        #     harvest_events,
        #     failed_events
        # )

        # if failed_events == 0:
        #     return True
        # else:
        #     return False
            
    except Exception as e:
        logger.exception("Unexpected error in run_harvester_oaipmh: %s", e)
        logger.info(
            "Harvest summary: processed %s records, successfully sent %s of them to the warehouse, failed to send %s records.",
            record_count,
            harvest_events,
            failed_events
        )
        return False
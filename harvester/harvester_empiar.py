import logging, os, httpx, xml.etree.ElementTree as ET, time, mimetypes, json
from datetime import datetime
from .db_api_functions import send_harvest_event
from xml.dom import minidom
from typing import Any, Dict, Iterator, Optional

_EMPIAR_CLIENT = httpx.Client(
    timeout = httpx.Timeout(120),
)

logger = logging.getLogger(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DEFAULT_BASE_URL = "https://www.ebi.ac.uk"
SEARCH_PATH = "/emdb/api/empiar/search/{query}"

PAGE_SIZE = 100



def search_page(page: int) -> Dict[str, Any]:
    """
    Fetch one page of results from `GET /emdb/api/empiar/search/{query}`.
 
    Queries the EMPIAR search endpoint with a wildcard query and requests
    all fields (`fl=*`) for the given page, using the module-level
    `PAGE_SIZE` as the row count.
 
    :param page: The number of page to fetch.
 
    :return: The parsed JSON payload for that page.
    """
    url = DEFAULT_BASE_URL + SEARCH_PATH.format(query = "*")
    params: Dict[str, Any] = {"rows": PAGE_SIZE, "page": page, "fl": "*"}
    response = _EMPIAR_CLIENT.get(url, params = params)
    response.raise_for_status()
    return response.json()



def search_all() -> Iterator[Dict[str, Any]]:
    """
    Walk every page of the EMPIAR search endpoint and yield each page's
    payload as it's fetched.
 
    :return: An iterator over page payloads, where each payload is a dict
            mapping EMPIAR IDs to their metadata records.
    """
    page = 1
    total_records = 0
    while True:
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

        page += 1
        time.sleep(2)



def empiar_additional_file_metadata(entry_id:  str) -> str:
    """
    Fetch the image-set metadata for a single EMPIAR entry

    :params: entry_id: the numeric EMPIAR id (e.g. "10050"), without the "EMPIAR-" prefix
    :return: the list of imagesets dictionaries as returned by the EMPIAR API
    """
    empiar_entry_endpoint = f"https://www.ebi.ac.uk/empiar/api/entry/EMPIAR-{entry_id}/"
    response = _EMPIAR_CLIENT.get(empiar_entry_endpoint)
    response.raise_for_status()
    payload = response.json()
    return payload[f"EMPIAR-{entry_id}"]["imagesets"]



def add_orcid(parent_el: ET.Element, orcid: str | None) -> None:
    """
    Attach a DataCite <nameIdentifier> element for an ORCID iD, if present.
 
    :param parent_el: The XML element (e.g. a `creator` or `contributor`
            element) to attach the `nameIdentifier` element to.
    :param orcid: The ORCID iD, either as a bare identifier or a full URL,
            or `None` if the person has no ORCID on record.
 
    :return: None. The `parent_el` element is mutated in place.
    """
    if not orcid:
        return
    orcid = orcid.strip()
    if not orcid:
        return
    ET.SubElement(
        parent_el,
        "nameIdentifier",
        nameIdentifierScheme = "ORCID",
        schemeURI = "https://orcid.org",
    ).text = orcid if orcid.startswith("http") else f"https://orcid.org/{orcid}"



def empiar_data_to_datacite(entry_id: str, record: Dict[str, Any]) -> tuple[str, str, str]:
    """
    Convert an EMPIAR entry record into a DataCite 4.6 XML record wrapped in
    an OAI-PMH <record> element.

    :param entry_id: The numeric EMPIAR id (e.g. "12345"), without the "EMPIAR-" prefix.
    :param record: Entry dictionary obtained from the EMPIAR API

    :return: A tuple containing:
            - xml_pretty (str): Formatted DataCite XML record.
            - datestamp (str): Record update/creation date (YYYY-MM-DD).
    """
    meta = record

    ET.register_namespace("", "http://www.openarchives.org/OAI/2.0/")
    ET.register_namespace("xsi", "http://www.w3.org/2001/XMLSchema-instance")

    creation_date = meta.get("deposition_date") or ""
    update_date = meta.get("update_date") or ""
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

    header = ET.SubElement(oai_record, "header", {"status": "deleted"} if obsolete_date else {})
    ET.SubElement(header, "identifier").text = f"EMPIAR-{entry_id}"
    datestamp_text = (update_date or creation_date)[:10]
    ET.SubElement(header, "datestamp").text = datestamp_text

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
    doi = meta.get("entry_doi") or ""
    doi_value = doi.split("doi:")[-1] if doi.lower().startswith("doi:") else doi
    if doi_value:
        ET.SubElement(resource, "identifier", identifierType="DOI").text = doi_value

    # CREATORS (mandatory) - authors, falling back to citation authors,
    # falling back to the principal investigator(s)
    author_entries = [
        (a["author"]["name"], a["author"].get("author_orcid"))
        for a in (meta.get("authors") or [])
        if a.get("author", {}).get("name")
    ]
    if not author_entries:
        for cit in meta.get("citation") or []:
            author_entries = [
                (a["name"], a.get("author_orcid")) for a in (cit.get("authors") or []) if a.get("name")
            ]
            if author_entries:
                break
    if not author_entries:
        author_entries = [
            (f"{pi.get('last_name', '')}, {pi.get('first_name', '')}".strip(", "), pi.get("author_orcid"))
            for pi in (meta.get("principal_investigator") or [])
            if pi.get("last_name") or pi.get("first_name")
        ]

    if author_entries:
        creators = ET.SubElement(resource, "creators")
        for name, orcid in author_entries:
            creator = ET.SubElement(creators, "creator")
            ET.SubElement(creator, "creatorName", nameType="Personal").text = name
            add_orcid(creator, orcid)

    # TITLES (mandatory)
    title = meta.get("title")
    titles = ET.SubElement(resource, "titles")
    ET.SubElement(titles, "title").text = title

    # PUBLISHER (mandatory)
    ET.SubElement(resource, "publisher").text = "Electron Microscopy Public Image Archive (EMPIAR)"

    # PUBLICATION YEAR (mandatory)
    pub_year_source = release_date or creation_date
    if pub_year_source:
        ET.SubElement(resource, "publicationYear").text = pub_year_source[:4]

    # RESOURCE TYPE (mandatory)
    resource_type_text = meta.get("experiment_type")
    if resource_type_text:
        ET.SubElement(resource, "resourceType", resourceTypeGeneral="Dataset").text = resource_type_text

    # LANGUAGE
    languages = {c.get("language") for c in (meta.get("citation") or []) if c.get("language")}
    if languages:
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
        name = f"{person.get('last_name', '')}, {person.get('first_name', '')}".strip(", ")
        if not name or name in seen_names:
            continue
        seen_names.add(name)
        contact_people.append((name, person))

    if contact_people:
        contributors = ET.SubElement(resource, "contributors")
        for name, person in contact_people:
            contributor = ET.SubElement(contributors, "contributor", contributorType="ContactPerson")
            ET.SubElement(contributor, "contributorName", nameType="Personal").text = name
            add_orcid(contributor, person.get("author_orcid"))
            if person.get("organization"):
                ET.SubElement(contributor, "affiliation").text = person["organization"]

    # DATES
    if creation_date or release_date or update_date:
        dates = ET.SubElement(resource, "dates")
        if creation_date:
            ET.SubElement(dates, "date", dateType="Submitted").text = creation_date[:10]
        if release_date:
            ET.SubElement(dates, "date", dateType="Available").text = release_date[:10]
        if update_date:
            ET.SubElement(dates, "date", dateType="Updated").text = update_date[:10]

    # RELATED IDENTIFIERS
    related_items = []

    if not doi_value:
        related_items.append(("URL", "IsIdentifiedBy", "https://www.ebi.ac.uk/empiar/EMPIAR-{entry_id}/".format(entry_id = entry_id)))

    for xref in meta.get("cross_references") or []:
        emd_id = xref.get("name") if isinstance(xref, dict) else xref
        if emd_id:
            related_items.append(("URL", "IsDerivedFrom", f"https://www.ebi.ac.uk/emdb/{emd_id}"))

    for cit in meta.get("citation") or []:
        if cit.get("doi"):
            cit_doi = cit["doi"]
            cit_doi_value = cit_doi.split("doi:")[-1] if cit_doi.lower().startswith("doi:") else cit_doi
            related_items.append(("DOI", "IsDocumentedBy", cit_doi_value))
        if cit.get("pubmedid"):
            related_items.append(("PMID", "IsDocumentedBy", cit["pubmedid"]))

    if related_items:
        related = ET.SubElement(resource, "relatedIdentifiers")
        for id_type, relation, text in related_items:
            ET.SubElement(
                related, "relatedIdentifier", relatedIdentifierType = id_type, relationType = relation
            ).text = text

    # SIZES
    dataset_size = meta.get("dataset_size")
    if dataset_size:
        sizes_el = ET.SubElement(resource, "sizes")
        ET.SubElement(sizes_el, "size").text = dataset_size

    # SUBJECTS
    categories = {img.get("category") for img in (meta.get("imagesets") or []) if img.get("category")}
    if categories:
        subjects_el = ET.SubElement(resource, "subjects")
        for cat in sorted(categories):
            ET.SubElement(subjects_el, "subject").text = cat

    # FORMATS
    formats_set = {
        img[key]
        for img in (meta.get("imagesets") or [])
        for key in ("header_format", "data_format")
        if img.get(key)
    }
    if formats_set:
        formats_el = ET.SubElement(resource, "formats")
        for fmt in sorted(formats_set):
            ET.SubElement(formats_el, "format").text = fmt

    # RIGHTS
    rights_list = ET.SubElement(resource, "rightsList")
    ET.SubElement(
        rights_list,
        "rights",
        rightsURI = "https://creativecommons.org/publicdomain/zero/1.0/",
        rightsIdentifier = "CC0-1.0",
    ).text = "CC0 1.0 Universal (CC0 1.0) Public Domain Dedication"

    # DESCRIPTIONS - built entirely from imageset name + details
    imageset_descriptions = []
    for img in meta.get("imagesets") or []:
        name = img.get("name")
        details = img.get("details")
        if name and details:
            imageset_descriptions.append(f"{name}: {details}")
        elif name or details:
            imageset_descriptions.append(name or details)

    if imageset_descriptions:
        descriptions = ET.SubElement(resource, "descriptions")
        for text in imageset_descriptions:
            ET.SubElement(descriptions, "description", descriptionType = "TechnicalInfo").text = text

    # FUNDING REFERENCES
    valid_grants = [g for g in (meta.get("grant_references") or []) if g.get("funding_body")]
    if valid_grants:
        funding_refs = ET.SubElement(resource, "fundingReferences")
        for grant in valid_grants:
            funding_ref = ET.SubElement(funding_refs, "fundingReference")
            ET.SubElement(funding_ref, "funderName").text = grant["funding_body"]
            if grant.get("code"):
                ET.SubElement(funding_ref, "awardNumber").text = grant["code"]

    # VERSION
    history = meta.get("version_history") or []
    if history:
        latest = history[-1]
        version_value = latest.get("version") if isinstance(latest, dict) else str(latest)
        if version_value:
            ET.SubElement(resource, "version").text = version_value

    xml_str = ET.tostring(oai_record, encoding = "unicode")
    xml_pretty = minidom.parseString(xml_str).toprettyxml(indent = "  ")

    return xml_pretty, datestamp_text



def run_harvester_empiar(run_info: dict) -> bool:
    """
    Run a full (or incremental) EMPIAR harvest and push
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
        projects = []
        if not from_date:
            projects = list(search_all())
        # else:
        #     projects = search_incremental(config)
        
        for page in projects:
            for empiar_key, record in page.items():
                entry_id = empiar_key.removeprefix("EMPIAR-")
                xml_out, datestamp = empiar_data_to_datacite(entry_id, record)
                additional_file_metadata = empiar_additional_file_metadata(entry_id)

                event_payload = {
                    "record_identifier": empiar_key,
                    "datestamp": datestamp,
                    "raw_metadata": xml_out,
                    "additional_metadata": json.dumps(additional_file_metadata),
                    "harvest_url": harvest_url,
                    "repo_code": "MDDB",
                    "harvest_run_id": run_info.get("id"),
                    "is_deleted": False
                }

                if send_harvest_event(event_payload):
                    harvest_events += 1
                else:
                    failed_events += 1

        logger.info(
            "Harvest summary: processed %s records, successfully sent %s of them to the warehouse, failed to send %s records.",
            record_count,
            harvest_events,
            failed_events
        )

        return failed_events == 0
            
    except Exception as e:
        logger.exception("Unexpected error in run_harvester_oaipmh: %s", e)
        logger.info(
            "Harvest summary: processed %s records, successfully sent %s of them to the warehouse, failed to send %s records.",
            record_count,
            harvest_events,
            failed_events
        )
        return False